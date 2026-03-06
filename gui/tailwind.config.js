/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx,ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        primary: { DEFAULT: "#135bec", hover: "#1d6bff", light: "#135bec20" },
        dark: {
          900: "#0c1018",
          800: "#101622",
          700: "#111722",
          600: "#1a2030",
          500: "#1e293b",
          400: "#1e293b",
        },
        sidebar: "#111722",
        surface: "#1e293b",
        "border-dark": "#1e293b",
        accent: { DEFAULT: "#6c5ce7", hover: "#7f71ed" },
      },
      fontFamily: {
        display: ["Inter", "system-ui", "sans-serif"],
        mono: ["JetBrains Mono", "SF Mono", "Fira Code", "Consolas", "monospace"],
      },
      borderRadius: { DEFAULT: "0.25rem", lg: "0.5rem", xl: "0.75rem" },
    },
  },
  plugins: [],
};
