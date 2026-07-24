import RNS

def canonical_message_bytes(msg_id: str, channel: str, sender_hash: str, timestamp: float, content: str) -> bytes:
    # Convert float timestamp to integer milliseconds to enforce deterministic string encoding
    ts_ms = int(float(timestamp) * 1000)
    raw = f"{msg_id}:{channel}:{sender_hash}:{ts_ms}:{content}"
    return raw.encode("utf-8")

def canonical_bulletin_bytes(bulletin_id: str, title: str, body: str, author_hash: str, timestamp: float) -> bytes:
    ts_ms = int(float(timestamp) * 1000)
    raw = f"{bulletin_id}:{title}:{body}:{author_hash}:{ts_ms}"
    return raw.encode("utf-8")

def canonical_profile_bytes(identity_hash: str, handle: str, status: str, bio: str, edited_at: float) -> bytes:
    ts_ms = int(float(edited_at) * 1000)
    raw = f"{identity_hash}:{handle}:{status}:{bio}:{ts_ms}"
    return raw.encode("utf-8")

def sign_bytes(identity: RNS.Identity, data: bytes) -> bytes:
    """
    Signs a canonical byte payload using the local RNS.Identity private key.

    :param identity: An active RNS.Identity object with private key capability.
    :param data: The canonicalized byte string payload to sign.
    :return: Raw Ed25519 signature bytes.
    :raises TypeError: If identity is invalid or missing signing capabilities.
    """
    if not identity or not hasattr(identity, "sign"):
        raise TypeError("A valid RNS.Identity instance with signing capability is required.")

    return identity.sign(data)

def verify_bytes(signer_hash_bytes: bytes, signature: bytes, data: bytes, public_key_bytes: bytes = None) -> bool:
    if not signature or not signer_hash_bytes:
        return False

    identity = None

    if public_key_bytes:
        try:
            if isinstance(public_key_bytes, str):
                public_key_bytes = bytes.fromhex(public_key_bytes)
            identity = RNS.Identity.from_bytes(public_key_bytes)
        except Exception:
            identity = None

    if identity is None:
        try:
            identity = RNS.Identity.recall(signer_hash_bytes)
        except Exception:
            identity = None

    if identity is None or identity.pub is None:
        return False

    try:
        return identity.validate(signature, data)
    except Exception:
        return False
