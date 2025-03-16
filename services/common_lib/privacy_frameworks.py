import logging
import time
import requests
from typing import Dict, Any, Set
# Instead of TCFv2Decoder, we import the top-level decode function from iab_tcf
from iab_tcf import decode as tcf_decode, iab_tcf

logger = logging.getLogger("householdiq_aggregator")

_gvl_data = None
_gvl_last_fetched = 0
_GVL_FETCH_INTERVAL = 60 * 60 * 6
DEFAULT_VENDOR_ID = 333
GVL_URL = "https://vendor-list.consensu.org/v2/vendor-list.json"

def download_global_vendor_list():
    global _gvl_data, _gvl_last_fetched
    try:
        resp = requests.get(GVL_URL, timeout=5)
        resp.raise_for_status()
        _gvl_data = resp.json()
        _gvl_last_fetched = time.time()
        logger.info(f"Fetched GVL version={_gvl_data.get('vendorListVersion')} from {GVL_URL}")
    except Exception as e:
        logger.error(f"Failed to fetch GVL: {e}")

def ensure_gvl_data():
    global _gvl_data, _gvl_last_fetched
    now = time.time()
    if _gvl_data is None or (now - _gvl_last_fetched) > _GVL_FETCH_INTERVAL:
        download_global_vendor_list()

def aggregator_vendor_consented(vendors_allowed: Set[int]) -> bool:
    return (DEFAULT_VENDOR_ID in vendors_allowed)

def parse_us_privacy_string(usp: str) -> Dict[str, Any]:
    data = {"version": None, "region": None, "opt_out_sale": None, "lspa": None}
    if not usp or len(usp) < 4:
        return data
    data["version"] = usp[0]
    data["region"] = usp[1]
    data["opt_out_sale"] = usp[2]
    data["lspa"] = usp[3]
    return data

def parse_tcf_string(tcf_string: str) -> Dict[str, Any]:
    """
    Uses iab_tcf's "decode(consent_str)" function to parse either v1.1 or v2 string.
    Then checks if aggregator is allowed, gathers vendor/purpose sets, etc.
    """

    data = {
        "valid": False,
        # We no longer have a direct "gdpr_applies" from iab_tcf, but you can set it if you wish
        "gdpr_applies": False,
        "purposes_allowed": set(),
        "vendors_allowed": set(),
        "special_features": set(),
        "vendor_consented": False
    }

    if not tcf_string:
        return data

    # If you wish to ensure your GVL is up to date first
    ensure_gvl_data()

    try:
        # The library auto-detects if it's v1 or v2
        consent_obj = tcf_decode(tcf_string)
        # By default, data["valid"] = True because we parsed it
        data["valid"] = True

        # Check if it's a v2 or v1 object. The docs say: 
        #   from iab_tcf.iab_tcf_v2 import ConsentV2
        #   from iab_tcf.iab_tcf_v1 import ConsentV1
        # We'll do a quick type check:
        from iab_tcf.iab_tcf_v1 import ConsentV1
        from iab_tcf.iab_tcf_v2 import ConsentV2

        # We can define a helper to iterate vendor or purpose IDs if needed
        def gather_purposes_v2(consent: ConsentV2):
            allowed = set()
            # TCF v2 typically has up to 24 official purposes
            for pid in range(1, 25):
                if consent.is_purpose_allowed(pid):
                    allowed.add(pid)
            return allowed

        def gather_vendors_v2(consent: ConsentV2):
            allowed = set()
            # The aggregator might not know all vendor IDs from GVL
            # but let's do a basic range. Some TCF vendor IDs can be quite large.
            # For example, try up to e.g. 1000. Or adapt to your default vendor set.
            # We'll do a small example range:
            for vid in range(1, 501):
                if consent.is_vendor_allowed(vid):
                    allowed.add(vid)
            return allowed

        if isinstance(consent_obj, ConsentV2):
            # We can set "gdpr_applies" if we assume v2 always => GDPR
            data["gdpr_applies"] = True

            # gather sets 
            data["purposes_allowed"] = gather_purposes_v2(consent_obj)
            data["vendors_allowed"] = gather_vendors_v2(consent_obj)

            # aggregator vendor check
            if consent_obj.is_vendor_allowed(DEFAULT_VENDOR_ID):
                data["vendor_consented"] = True

            # We could attempt to gather "special features" but the library doesn't 
            # appear to provide a direct method. If needed, you'd parse them from 
            # the raw bit data or check code in "ConsentV2".
            # For now, let's skip it or do a placeholder approach.

        elif isinstance(consent_obj, ConsentV1):
            # v1 doesn't define "gdpr_applies" either, but we might assume True
            data["gdpr_applies"] = True

            # v1 approach: 
            # we can gather vendor/purpose with is_vendor_allowed, is_purpose_allowed.
            for pid in range(1, 25):
                if consent_obj.is_purpose_allowed(pid):
                    data["purposes_allowed"].add(pid)

            # same vendor range logic:
            for vid in range(1, 501):
                if consent_obj.is_vendor_allowed(vid):
                    data["vendors_allowed"].add(vid)

            if consent_obj.is_vendor_allowed(DEFAULT_VENDOR_ID):
                data["vendor_consented"] = True

        else:
            # If it can't detect v1 or v2, we might leave data as is
            logger.warning("iab_tcf.decode returned an unknown consent object type.")

    except Exception as e:
        logger.warning(f"Failed to parse TCF string with iab_tcf.decode: {e}")

    return data

# GPP references commented out:
# def parse_gpp_string(...): pass

def user_allows_bridging(
    tcf_data: Dict[str, Any],
    us_privacy_data: Dict[str, Any],
    # gpp_data: Dict[str, Any]
) -> bool:

    # 1) TCF checks
    if tcf_data.get("valid"):
        # aggregator vendor must be consented
        if not tcf_data.get("vendor_consented"):
            return False
        required_purposes = {1, 2}
        if not required_purposes.issubset(tcf_data["purposes_allowed"]):
            return False

    # 2) US Privacy (CCPA / CPRA) checks
    if us_privacy_data.get("region") == 'C' and us_privacy_data.get("opt_out_sale") == 'Y':
        return False

    # 3) GPP logic removed or commented out for now

    return True
