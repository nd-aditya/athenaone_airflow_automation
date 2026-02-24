import type { Config } from "tailwindcss";

export default {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        background: "#F1F2F7",
        foreground: "var(--foreground)",
        mainText: "#2D3453",
        secText: "#60667D",
        primary: "#0A6475",
        secondary: "#0B9F8D",
        riceFlower: "#EAFFDB",
        borderColor: "#CCCFDB",
        athensGray: "#E9ECF1",
        casablanca: "#FAB84E",
        bittersweet: "#FF7575",
        bostonBlue: "#309BAE",
        whiteIce: "#E5FAF4",
        lemonChiffon: "#FFFAC7",
        mischka: "#D9DCE8",
      },
      fontFamily: {
        sans: ['ui-sans-serif', 'system-ui', 'sans-serif'], // fallback-safe
      }
    }, 
  },
  plugins: [],
} satisfies Config;
