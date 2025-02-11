import hashlib

from awesome_api.models import ClaimSize, ClaimStatus


def hash_claim_id(claim_id: str) -> str:
    return str(hashlib.sha256(claim_id.encode()).hexdigest())


def set_claim_status(
    initial_claim_amount: int, current_claim_amount: int
) -> ClaimStatus:
    if current_claim_amount == 0:
        return ClaimStatus.SETTLED

    elif current_claim_amount <= initial_claim_amount / 2:
        return ClaimStatus.PARTIALLY_SETTLED

    else:
        return ClaimStatus.NOT_SETTLED


def set_claim_size(initial_claim_amount: int) -> ClaimSize:
    if initial_claim_amount < 20000:
        return ClaimSize.XS
    elif (initial_claim_amount < 30000) and (initial_claim_amount >= 20000):
        return ClaimSize.S
    elif (initial_claim_amount < 100000) and (initial_claim_amount >= 30000):
        return ClaimSize.M
    elif (initial_claim_amount < 500000) and (initial_claim_amount >= 100000):
        return ClaimSize.L
    elif initial_claim_amount >= 500000:
        return ClaimSize.XL
