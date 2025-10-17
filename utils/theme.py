import pandas as pd
import streamlit as st

DEFAULT_THEME = {
    "primary": "#FF6A00",     # EXL Orange-ish fallback
    "text": "#1F1F1F",
    "bg": "#FFFFFF",
    "bg2": "#FAFAFA",
    "palette": [
        "#FF6A00", "#004B87", "#FFB000", "#4C8C2B", "#8E44AD",
        "#C0392B", "#16A085", "#2C3E50", "#D35400", "#7F8C8D",
        "#3498DB", "#27AE60"
    ]
}

def load_theme_tokens(csv_path: str) -> dict:
    """
    Load color tokens from a CSV (any two-column shape).
    Tries to sniff names (e.g., primary, text, bg, bg2, chart1..N).
    Falls back to DEFAULT_THEME if anything fails.
    """
    try:
        df = pd.read_csv(csv_path)
        if df.shape[1] < 2:
            return DEFAULT_THEME
        # normalize columns
        cols = list(df.columns)
        name_col, val_col = cols[0], cols[1]
        df[name_col] = df[name_col].astype(str).str.strip().str.lower()
        df[val_col] = df[val_col].astype(str).str.strip()

        # basic picks
        primary = _pick(df, name_col, val_col, ["primary", "brand", "accent"]) or DEFAULT_THEME["primary"]
        text    = _pick(df, name_col, val_col, ["text", "foreground"]) or DEFAULT_THEME["text"]
        bg      = _pick(df, name_col, val_col, ["bg", "background"]) or DEFAULT_THEME["bg"]
        bg2     = _pick(df, name_col, val_col, ["bg2","surface","secondarybackground"]) or DEFAULT_THEME["bg2"]

        # palette
        palette = []
        for idx in range(1, 30):
            c = _pick(df, name_col, val_col, [f"chart{idx}", f"color{idx}", f"series{idx}"])
            if c: palette.append(c)
        if not palette:
            # fallback: all hex-looking values from CSV
            palette = [v for v in df[val_col].tolist() if v.startswith("#")]
        if not palette:
            palette = DEFAULT_THEME["palette"]

        return {"primary": primary, "text": text, "bg": bg, "bg2": bg2, "palette": palette}
    except Exception:
        return DEFAULT_THEME

def _pick(df, key_col, val_col, names):
    row = df[df[key_col].isin([n.lower() for n in names])]
    if not row.empty:
        return row.iloc[0][val_col]
    return None

def apply_theme_css(theme: dict):
    """Light-touch CSS to improve headings and buttons without fighting Streamlit theming."""
    primary = theme["primary"]; text = theme["text"]; bg = theme["bg"]
    st.markdown(f"""
    <style>
      :root {{
        --brand-primary: {primary};
        --brand-text: {text};
        --brand-bg: {bg};
      }}
      h2, h3 {{ color: var(--brand-text); }}
      .stButton>button {{
        background: var(--brand-primary);
        color: white;
        border: none;
      }}
      .stMetricDelta-positive {{
        color: var(--brand-primary) !important;
      }}
    </style>
    """, unsafe_allow_html=True)

def pick_palette(theme: dict, need: int = 8) -> list:
    pal = theme.get("palette", [])[:need]
    if len(pal) < need:
        pal = pal + DEFAULT_THEME["palette"][:(need - len(pal))]
    return pal
