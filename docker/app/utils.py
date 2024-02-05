import ssl


def map_tls_version(tls_version_str: str):
    version_mapping = {
        "TLSv1.2": ssl.PROTOCOL_TLSv1_2,
        "TLSv1.1": ssl.PROTOCOL_TLSv1_1,
        # Add more mappings if needed
    }
    return version_mapping.get(tls_version_str, ssl.PROTOCOL_TLS)
