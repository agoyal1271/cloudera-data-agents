/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        // Cloudera navy layer system — dark enterprise background
        cdp: {
          950: '#0B1520',  // deepest background
          900: '#102030',  // panel background
          800: '#162840',  // card / elevated surface
          700: '#1e3a55',  // border / divider
          600: '#2a4f72',  // muted border hover
        },
        // Cloudera orange — signature brand color
        cloudera: {
          DEFAULT: '#F96702',
          hover:   '#FF7D1A',
          muted:   '#C24F00',
          faint:   '#2A1404',
        },
        // AI Agent Platform Theme — Premium Enterprise Palette
        agent: {
          'dark-bg': '#0F141C',      // The Deep Charcoal — main page background
          'dark-surface': '#171E29', // The Interface Cards — panels, cards
          'dark-border': '#242F3E',  // The Structural Lines — dividers, borders
          'text-primary': '#F3F4F6',   // Primary text
          'text-secondary': '#9CA3AF', // Secondary/muted text
          'orange': '#FF5B00',  // The Innovation Orange — CTAs, highlights
          'teal': '#00A3C4',    // The Tech Teal — data flows, metrics
        },
      },
      fontSize: {
        // iOS-like reading scale — 14px floor, 16px body, generous line-height
        'xs':  ['0.875rem',  { lineHeight: '1.375rem' }],  // 14px — small labels / secondary
        'sm':  ['1rem',      { lineHeight: '1.625rem' }],  // 16px — primary body / chat
        'base':['1.125rem',  { lineHeight: '1.75rem'  }],  // 18px — emphasis / headings
        'lg':  ['1.3125rem', { lineHeight: '1.875rem' }],  // 21px
        'xl':  ['1.5rem',    { lineHeight: '2rem'     }],  // 24px
      },
    },
  },
  plugins: [],
}
