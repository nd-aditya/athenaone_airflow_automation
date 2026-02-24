# utils/text_cleaning.py
import re
import unicodedata

def clean_text(text: str) -> str:
    """
    Normalize and clean clinical text before mapping to standard codes.

    Steps:
    1. Convert Unicode to ASCII (remove accents, normalize symbols)
    2. Lowercase for consistency
    3. Remove extra spaces
    4. Remove non-alphanumeric characters except medical-relevant ones
    5. Strip trailing spaces
    """
    if not text:
        return ""

    # Convert to string just in case
    text = str(text)

    # Normalize unicode characters (é → e, μ → micro)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()

    # Lowercase
    text = text.lower()

    # Remove non-alphanumeric except for allowed chars (keep space, %, /, -, .)
    text = re.sub(r"[^a-z0-9 %/.\-]", " ", text)

    # Collapse multiple spaces
    text = re.sub(r"\s+", " ", text)

    # Strip spaces
    text = text.strip()

    return text
