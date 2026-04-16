#!/bin/bash
# =============================================================================
# setup_vpn.sh — One-time VPN setup for Airflow DAG automation
# Run once on each MAC machine:
#   chmod +x setup_vpn.sh
#   ./setup_vpn.sh
# =============================================================================

set -e

USERNAME=$(whoami)
PROFILES_DIR="$HOME/Library/Application Support/OpenVPN Connect/profiles"
CONFIG_DEST="/etc/openvpn/nd-platform.ovpn"

# Detect openvpn path (Apple Silicon vs Intel)
if [ -f "/opt/homebrew/sbin/openvpn" ]; then
    OPENVPN_BIN="/opt/homebrew/sbin/openvpn"
elif [ -f "/usr/local/sbin/openvpn" ]; then
    OPENVPN_BIN="/usr/local/sbin/openvpn"
else
    OPENVPN_BIN=""
fi

echo "============================================"
echo " OpenVPN DAG Setup"
echo " User     : $USERNAME"
echo "============================================"


# ── Step 1: Install OpenVPN via brew ─────────────────────────────────────────
echo ""
echo "[1/5] Checking OpenVPN installation..."
if [ -n "$OPENVPN_BIN" ]; then
    echo "      Already installed at $OPENVPN_BIN — skipping."
else
    echo "      Installing via brew..."
    brew install openvpn
    # Re-detect after install
    if [ -f "/opt/homebrew/sbin/openvpn" ]; then
        OPENVPN_BIN="/opt/homebrew/sbin/openvpn"
    else
        OPENVPN_BIN="/usr/local/sbin/openvpn"
    fi
    echo "      Installed at $OPENVPN_BIN"
fi


# ── Step 2: Copy .ovpn profile ────────────────────────────────────────────────
echo ""
echo "[2/5] Copying OpenVPN profile..."

# Find .ovpn files in profiles dir
OVPN_FILES=("$PROFILES_DIR"/*.ovpn)
OVPN_COUNT=${#OVPN_FILES[@]}

if [ "$OVPN_COUNT" -eq 0 ]; then
    echo "      ERROR: No .ovpn profile found in:"
    echo "      $PROFILES_DIR"
    echo "      Please import a profile in the OpenVPN Connect app first."
    exit 1
elif [ "$OVPN_COUNT" -gt 1 ]; then
    echo "      Multiple profiles found:"
    for i in "${!OVPN_FILES[@]}"; do
        echo "        [$i] ${OVPN_FILES[$i]}"
    done
    read -p "      Enter number to use [0]: " CHOICE
    CHOICE=${CHOICE:-0}
    OVPN_SRC="${OVPN_FILES[$CHOICE]}"
else
    OVPN_SRC="${OVPN_FILES[0]}"
fi

echo "      Using profile: $OVPN_SRC"
sudo mkdir -p /etc/openvpn
sudo cp "$OVPN_SRC" "$CONFIG_DEST"
echo "      Copied to $CONFIG_DEST"


# ── Step 3: Remove unsupported option ────────────────────────────────────────
echo ""
echo "[3/5] Removing unsupported 'block-outside-dns' option..."
if sudo grep -q "block-outside-dns" "$CONFIG_DEST"; then
    sudo sed -i '' '/block-outside-dns/d' "$CONFIG_DEST"
    echo "      Removed."
else
    echo "      Not present — skipping."
fi


# ── Step 4: Configure sudoers ─────────────────────────────────────────────────
echo ""
echo "[4/5] Configuring passwordless sudo for openvpn..."

SUDOERS_LINE="$USERNAME ALL=(ALL) NOPASSWD: $OPENVPN_BIN, /usr/bin/killall"
SUDOERS_FILE="/private/etc/sudoers.d/openvpn_airflow"

# Write to a separate sudoers drop-in file (safer than editing sudoers directly)
echo "$SUDOERS_LINE" | sudo tee "$SUDOERS_FILE" > /dev/null
sudo chmod 440 "$SUDOERS_FILE"
echo "      Sudoers rule written to $SUDOERS_FILE"


# ── Step 5: Update vpn_service.py with correct binary path ───────────────────
echo ""
echo "[5/5] Updating OPENVPN_BIN in services/vpn_service.py..."

VPN_SERVICE="$(dirname "$0")/services/vpn_service.py"
if [ -f "$VPN_SERVICE" ]; then
    sed -i '' "s|OPENVPN_BIN.*=.*|OPENVPN_BIN    = \"$OPENVPN_BIN\"|" "$VPN_SERVICE"
    echo "      Updated $VPN_SERVICE"
else
    echo "      WARNING: $VPN_SERVICE not found — update OPENVPN_BIN manually."
fi


# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "============================================"
echo " Setup complete!"
echo ""
echo " Test with:"
echo "   conda activate py39"
echo "   python test_vpn_snowflake.py"
echo "============================================"
