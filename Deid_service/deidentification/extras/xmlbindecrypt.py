from sqlalchemy import create_engine, text
import pandas as pd
from lxml import etree
import re, os, zlib, gzip, binascii
from decimal import Decimal

engine = create_engine(
    "mssql+pyodbc://sa:ndADMIN2025@localhost,1433/master?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes",
    pool_pre_ping=True,
    fast_executemany=True
)

bintypeid_to_ext = {
    1004: "xml",  # DocumentXML
    1000: "xml",  # XML
    1001: "pdf",  # PDF
    1016: "xml",  # FreeTextXML
    1003: "txt",  # TXT
    1005: "xml",  # CDADocumentXML
    1007: "tif",  # TIF
}

def clean_xml(xml_bytes):
    """Decode, strip illegal characters, and repair bad XML if possible."""
    xml_string = xml_bytes.decode("utf-8", errors="ignore")
    xml_string = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", xml_string)

    try:
        parser = etree.XMLParser(recover=True)
        root = etree.fromstring(xml_string.encode("utf-8"), parser)
        return etree.tostring(root, encoding="utf-8").decode("utf-8")
    except Exception:
        return None

def normalize_binary(binary_data):
    """Detect encoding/compression type and return raw bytes."""
    if not binary_data:
        return None

    # Case 1: hex string starting with "0x"
    if isinstance(binary_data, (bytes, bytearray)) and binary_data.startswith(b"0x"):
        try:
            binary_data = binascii.unhexlify(binary_data[2:])
        except Exception:
            return None

    # Case 2: zlib (0x78 0x9C etc.)
    if binary_data.startswith(b"\x78"):
        try:
            return zlib.decompress(binary_data)
        except Exception:
            pass

    # Case 3: gzip (0x1F 0x8B)
    if binary_data.startswith(b"\x1F\x8B"):
        try:
            return gzip.decompress(binary_data)
        except Exception:
            pass

    # Default: return as-is
    return binary_data

batch_size = 1000
offset = 0


def normalize_doc_id(doc_id_str: str) -> str:
    try:
        # Check if it looks like scientific notation
        if re.match(r"^[0-9.]+e[+-]?[0-9]+$", doc_id_str, re.IGNORECASE):
            # Use Decimal for exact conversion
            return str(Decimal(doc_id_str).quantize(Decimal("1")))
        return doc_id_str
    except Exception:
        return doc_id_str


with engine.connect() as conn:
    while True:
        query = text(f"""
            SELECT CAST(DocumentID as nvarchar(50)) AS DocumentID, SequenceNumber, BinTypeID, DocImage, nd_auto_increment_id
            FROM dbo.ClinicalBin_Suv_Bio_Epi
            WHERE BinTypeID IN (1016, 1005, 1004, 1000)
            ORDER BY DocumentID
            OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY;
        """)
        results = conn.execute(query).fetchall()
        print(f"📦 Fetched {len(results)} records (offset {offset})")

        if not results:
            print("✅ Finished all records")
            break

        insert_data = []

        for row in results:
            raw = row.DocumentID  # already a string
            doc_id = normalize_doc_id(raw)
            seq_no = row.SequenceNumber
            bintypeid = int(row.BinTypeID)
            binary_data = row.DocImage
            auto_increment_id = row.nd_auto_increment_id
            file_ext = bintypeid_to_ext.get(bintypeid, "bin")

            try:
                normalized = normalize_binary(binary_data)

                if not normalized:
                    print(f"⏭ Skipped unreadable data for {doc_id}_{seq_no}_{bintypeid}")
                    continue

                if file_ext == "xml":
                    xml_string = clean_xml(normalized)
                    if xml_string:
                        insert_data.append({
                            "DocumentID": str(doc_id),
                            "SequenceNumber": seq_no,
                            "BinTypeID": bintypeid,
                            "DocContent": xml_string,
                            "nd_auto_increment_id": int(auto_increment_id)
                        })
                    else:
                        print(f"⏭ Invalid XML for {doc_id}_{seq_no}_{bintypeid}")
                else:
                    # Non-XML: try UTF-8 decode, fallback latin-1
                    if isinstance(normalized, bytes):
                        try:
                            doc_string = normalized.decode("utf-8", errors="ignore")
                        except Exception:
                            doc_string = normalized.decode("latin-1", errors="ignore")
                    else:
                        doc_string = str(normalized)

                    insert_data.append({
                        "DocumentID": str(doc_id),
                        "SequenceNumber": seq_no,
                        "BinTypeID": bintypeid,
                        "DocContent": doc_string,
                        "nd_auto_increment_id": int(auto_increment_id)
                    })

            except Exception as e:
                print(f"❌ Error processing {doc_id}_{seq_no}_{bintypeid}: {e}")

        if insert_data:
            conn.execute(text("""
                INSERT INTO ClinicalBin_Suv_Bio_Epi_decrypt_2
                (DocumentID, SequenceNumber, BinTypeID, DocContent, nd_auto_increment_id)
                VALUES (:DocumentID, :SequenceNumber, :BinTypeID, :DocContent, :nd_auto_increment_id)
            """), insert_data)
            conn.commit()
            print(f"✅ Inserted {len(insert_data)} cleaned records")

        offset += batch_size
