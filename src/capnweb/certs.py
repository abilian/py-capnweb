"""Certificate management utilities for WebTransport/HTTP/3.

This module provides utilities for generating self-signed certificates
for development and testing WebTransport connections.
"""

from __future__ import annotations

import datetime
import ipaddress
from pathlib import Path
from typing import TYPE_CHECKING

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtensionOID, NameOID

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey


def generate_self_signed_cert(
    hostname: str = "localhost",
    key_size: int = 2048,
    validity_days: int = 365,
    output_dir: Path | None = None,
) -> tuple[Path, Path]:
    """Generate a self-signed certificate for WebTransport.

    This generates a certificate suitable for development and testing.
    For production, use properly CA-signed certificates.

    Args:
        hostname: The hostname for the certificate (default: "localhost")
        key_size: RSA key size in bits (default: 2048)
        validity_days: Certificate validity in days (default: 365)
        output_dir: Directory to save cert files (default: current directory)

    Returns:
        Tuple of (cert_path, key_path) - paths to the generated files

    Example:
        ```python
        cert_path, key_path = generate_self_signed_cert("localhost")
        print(f"Certificate: {cert_path}")
        print(f"Private key: {key_path}")
        ```
    """
    output_dir = Path.cwd() if output_dir is None else Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate private key
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=key_size,
    )

    # Build subject and issuer (same for self-signed)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Cap'n Web"),
        x509.NameAttribute(NameOID.COMMON_NAME, hostname),
    ])

    # Build certificate
    cert = (
        x509
        .CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.UTC))
        .not_valid_after(
            datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=validity_days)
        )
        .add_extension(
            x509.SubjectAlternativeName(_build_san_list(hostname)),
            critical=False,
        )
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=True,
        )
        .sign(private_key, hashes.SHA256())
    )

    # Write certificate
    cert_path = output_dir / f"{hostname}.crt"
    Path(cert_path).write_bytes(cert.public_bytes(serialization.Encoding.PEM))

    # Write private key
    key_path = output_dir / f"{hostname}.key"
    Path(key_path).write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )

    return cert_path, key_path


def _build_san_list(hostname: str) -> list[x509.GeneralName]:
    """Build Subject Alternative Name list for the certificate.

    Args:
        hostname: The hostname to include in SAN

    Returns:
        List of GeneralName entries for the SAN extension
    """
    san_list: list[x509.GeneralName] = []

    # Add DNS name
    san_list.append(x509.DNSName(hostname))

    # If it's localhost, also add 127.0.0.1 and ::1
    if hostname == "localhost":
        san_list.extend((
            x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
            x509.IPAddress(ipaddress.IPv6Address("::1")),
        ))

    # Try to parse as IP address
    try:
        ip = ipaddress.ip_address(hostname)
        san_list.append(x509.IPAddress(ip))
    except ValueError:
        # Not an IP address, that's fine
        pass

    return san_list


def load_certificate(cert_path: Path | str) -> x509.Certificate:
    """Load a certificate from a PEM file.

    Args:
        cert_path: Path to the certificate file

    Returns:
        The loaded certificate

    Raises:
        FileNotFoundError: If the certificate file doesn't exist
        ValueError: If the file is not a valid PEM certificate
    """
    cert_path = Path(cert_path)
    cert_data = Path(cert_path).read_bytes()

    return x509.load_pem_x509_certificate(cert_data)


def load_private_key(key_path: Path | str) -> RSAPrivateKey:
    """Load a private key from a PEM file.

    Args:
        key_path: Path to the private key file

    Returns:
        The loaded private key

    Raises:
        FileNotFoundError: If the key file doesn't exist
        ValueError: If the file is not a valid PEM private key
    """
    key_path = Path(key_path)
    key_data = Path(key_path).read_bytes()

    key = serialization.load_pem_private_key(key_data, password=None)

    if not isinstance(key, rsa.RSAPrivateKey):
        msg = "Only RSA private keys are supported"
        raise ValueError(msg)

    return key


def verify_certificate(cert: x509.Certificate, hostname: str) -> bool:
    """Verify that a certificate is valid for a given hostname.

    Args:
        cert: The certificate to verify
        hostname: The hostname to verify against

    Returns:
        True if the certificate is valid for the hostname
    """
    # Check validity period
    now = datetime.datetime.now(datetime.UTC)
    if now < cert.not_valid_before_utc or now > cert.not_valid_after_utc:
        return False

    # Check Subject Alternative Name
    try:
        san_ext = cert.extensions.get_extension_for_oid(
            ExtensionOID.SUBJECT_ALTERNATIVE_NAME
        )
        san_list = san_ext.value

        # Try to match hostname
        for name in san_list:  # type: ignore[attr-defined]
            if isinstance(name, x509.DNSName) and name.value == hostname:
                return True
            if isinstance(name, x509.IPAddress):
                try:
                    ip = ipaddress.ip_address(hostname)
                    if name.value == ip:
                        return True
                except ValueError:
                    # hostname is not an IP
                    pass

    except x509.ExtensionNotFound:
        # No SAN extension, check Common Name
        cn_attrs = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        if cn_attrs and cn_attrs[0].value == hostname:
            return True

    return False
