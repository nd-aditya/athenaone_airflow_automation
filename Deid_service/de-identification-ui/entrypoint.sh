#!/bin/sh

# Start Next.js as 'nextjs' user
su -s /bin/sh nextjs -c "node server.js &"

# Start Nginx in the foreground as root
exec nginx -g 'daemon off;'