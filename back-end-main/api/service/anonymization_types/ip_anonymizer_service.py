import logging
import sys
from array import array
from functools import reduce

from Crypto.Cipher import AES
from sqlalchemy import select

from service.database_service import (
    create_table_session,
    get_index_column_table_object,
    get_primary_key,
)

if sys.version_info < (3, 3):
    import netaddr
else:
    import ipaddress

_logger = logging.getLogger(__name__)


class AddressValueError(ValueError):
    """Exception class raised when the IP address parser (the netaddr
    module in Python < 3.3 or ipaddress module) failed.
    """

    pass


class CryptoPAn(object):
    """Anonymize IP addresses keepting prefix consitency."""

    def __init__(self, key):
        """Initialize a CryptoPAn() instance.
        Args:
            key: a 32 bytes object used for AES key and padding when
                 performing a block cipher operation. The first 16 bytes
                 are used for the AES key, and the latter for padding.
        Changelog: A bytes object (not string) is required for python3.
        """
        assert len(key) == 32
        if sys.version_info.major < 3:
            assert type(key) is str
        else:
            assert type(key) is bytes
        self._cipher = AES.new(key[:16], AES.MODE_ECB)
        self._padding = array("B")
        if sys.version_info.major == 2:
            # for Python2
            self._padding.fromstring(self._cipher.encrypt(key[16:]))
        else:
            # for Python3 (and later?)
            self._padding.frombytes(self._cipher.encrypt(key[16:]))
        self._padding_int = self._to_int(self._padding)
        self._gen_masks()

    def _gen_masks(self):
        """Generates an array of bit masks to calculate n-bits padding data."""
        mask128 = reduce(lambda x, y: (x << 1) | y, [1] * 128)
        self._masks = [0] * 128
        for l in range(128):
            # self._masks[0]   <- 128bits all 1
            # self._masks[127] <- 1
            self._masks[l] = mask128 >> l

    def _to_array(self, int_value, int_value_len):
        """Convert an int value to a byte array."""
        byte_array = array("B")
        for i in range(int_value_len):
            byte_array.insert(0, (int_value >> (i * 8)) & 0xFF)
        return byte_array

    def _to_int(self, byte_array):
        """Convert a byte array to an int value."""
        return reduce(lambda x, y: (x << 8) | y, byte_array)

    def anonymize(self, addr):
        """Anonymize an IP address represented as a text string.
        Args:
            addr: an IP address string.
        Returns:
            An anoymized IP address string.
        """
        aaddr = None
        if sys.version_info < (3, 3):
            # for Python before 3.3
            try:
                ip = netaddr.IPNetwork(addr)
            except netaddr.AddrFormatError:
                raise AddressValueError
            aaddr = self.anonymize_bin(ip.value, ip.version)
        else:
            # for newer Python3 (and later?)
            try:
                ip = ipaddress.ip_address(addr)
            except (ValueError, ipaddress.AddressValueError) as e:
                raise AddressValueError
            aaddr = self.anonymize_bin(int(ip), ip.version)
        if ip.version == 4:
            return "%d.%d.%d.%d" % (
                aaddr >> 24,
                (aaddr >> 16) & 0xFF,
                (aaddr >> 8) & 0xFF,
                aaddr & 0xFF,
            )
        else:
            return "%x:%x:%x:%x:%x:%x:%x:%x" % (
                aaddr >> 112,
                (aaddr >> 96) & 0xFFFF,
                (aaddr >> 80) & 0xFFFF,
                (aaddr >> 64) & 0xFFFF,
                (aaddr >> 48) & 0xFFFF,
                (aaddr >> 32) & 0xFFFF,
                (aaddr >> 16) & 0xFFFF,
                aaddr & 0xFFFF,
            )

    def anonymize_bin(self, addr, version):
        """Anonymize an IP address represented as an integer value.
        Args:
            addr: an IP address value.
            version: the version of the address (either 4 or 6)
        Returns:
            An anoymized IP address value.
        """
        assert version == 4 or version == 6
        if version == 4:
            pos_max = 32
            ext_addr = addr << 96
        else:
            pos_max = 128
            ext_addr = addr

        flip_array = []
        for pos in range(pos_max):
            prefix = ext_addr >> (128 - pos) << (128 - pos)
            padded_addr = prefix | (self._padding_int & self._masks[pos])
            if sys.version_info.major == 2:
                # for Python2
                f = self._cipher.encrypt(self._to_array(padded_addr, 16).tostring())
            else:
                # for Python3 (and later?)
                f = self._cipher.encrypt(self._to_array(padded_addr, 16).tobytes())
            flip_array.append(bytearray(f)[0] >> 7)
        result = reduce(lambda x, y: (x << 1) | y, flip_array)

        return addr ^ result


def anonymization_ip(ip):
    cp = CryptoPAn("".join([chr(x) for x in range(0, 32)]).encode())
    return cp.anonymize(str(ip))


def anonymization_data(
    rows_to_anonymize: list[dict],
    columns_to_anonymize: list[str],
) -> list[dict]:
    """
    This function anonymizes each column of each given row with
    the Ip Anonymizer.

    Parameters
    ----------
    row_to_anonymize : list[dict]
        Rows provided for anonymization.

    columns_to_anonymize : list[str]
        Column names chosen for anonymization.

    Returns
    -------
    list[dict]
        Anonymized rows
    """

    for number_row in range(len(rows_to_anonymize)):

        # Anonymize ip
        for column in columns_to_anonymize:
            rows_to_anonymize[number_row][column] = anonymization_ip(
                rows_to_anonymize[number_row][column]
            )

    return rows_to_anonymize


def anonymization_database_rows(
    src_client_db_path: str,
    table_name: str,
    columns_to_anonymize: list[str],
    rows_to_anonymize: list[dict],
    insert_database: bool = True,
) -> list[dict]:
    """
    This function anonymizes each the given rows with the
    Ip Anonymizer.

    Parameters
    ----------
    src_client_db_path : str
        Connection URI of Client Database.

    table_name : str
        Table name where anonymization will be performed.

    columns_to_anonymize : list[str]
        Column names chosen for anonymization.

    rows_to_anonymize : list[dict]
        Rows provided for anonymization.

    insert_database : bool
        Flag to indicate if anonymized rows will be inserted or returned.

    Returns
    -------
    list[dict]
        Anonymized rows
    """

    # Get primary key column name of Client Database
    primary_key_name = get_primary_key(
        src_db_path=src_client_db_path, table_name=table_name
    )

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, table_name=table_name
    )

    index_primary_key = get_index_column_table_object(
        table_object=table_client_db,
        column_name=primary_key_name,
    )

    # Run anonymization
    anonymized_rows = anonymization_data(
        rows_to_anonymize=rows_to_anonymize,
        columns_to_anonymize=columns_to_anonymize,
    )

    # Insert anonymized rows in database
    if insert_database:
        for anonymized_row in anonymized_rows:
            session_client_db.query(table_client_db).filter(
                table_client_db.c[index_primary_key]
                == anonymized_row[f"{primary_key_name}"]
            ).update({key: anonymized_row[key] for key in columns_to_anonymize})

        session_client_db.commit()

    session_client_db.close()

    return anonymized_rows


def anonymization_database(
    src_client_db_path: str, table_name: str, columns_to_anonymize: list[str]
) -> int:
    """
    This function anonymizes all database rows from all tables with the
    Ip Anonymizer.

    Parameters
    ----------
    src_client_db_path : str
        Connection URI of Client Database

    table_name : str
        Table name where anonymization will be performed

    columns_to_anonymize : list[str]
        Column names chosen for anonymization.

    Returns
    -------
    int
        200 (Success code)
    """

    # Get primary key column name of Client Database
    primary_key_name = get_primary_key(
        src_db_path=src_client_db_path, table_name=table_name
    )

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, table_name=table_name
    )

    index_primary_key = get_index_column_table_object(
        table_object=table_client_db,
        column_name=primary_key_name,
    )

    # Get rows on batch  for anonymization
    size = 1000
    statement = select(table_client_db)
    results_proxy = session_client_db.execute(
        statement  # Proxy to get rows on batch  for anonymization
    )
    results = results_proxy.fetchmany(size)  # Getting rows for anonymization

    while results:
        # Transform rows from tuples to dictionary
        rows_to_anonymize = [row._asdict() for row in results]

        # Run anonymization
        anonymized_rows = anonymization_data(
            rows_to_anonymize=rows_to_anonymize,
            columns_to_anonymize=columns_to_anonymize,
        )

        # Insert anonymized rows in database
        for anonymized_row in anonymized_rows:
            session_client_db.query(table_client_db).filter(
                table_client_db.c[index_primary_key]
                == anonymized_row[f"{primary_key_name}"]
            ).update({key: anonymized_row[key] for key in columns_to_anonymize})

        session_client_db.commit()

        # Getting rows for anonymization
        results = results_proxy.fetchmany(size)

    session_client_db.close()

    return 200
