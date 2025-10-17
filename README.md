# UK Broadband – Price & Speed Comparison

Streamlit app that checks UK broadband deals by postcode across multiple ISPs, navigates address/moving steps, respects robots.txt (toggle), caches results to CSV for 24h reuse, and visualizes entry price + price vs speed. Includes a comparison table.

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install
streamlit run app.py
