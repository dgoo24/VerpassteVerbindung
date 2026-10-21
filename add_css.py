path = r"C:\Users\dgoo2\MVP1_EventosYStreamingEnTiempoReal\train-missed-connection-mvp\src\dashboard_app.py"
with open(path, encoding="utf-8") as f:
    s = f.read()
css = '''    st.markdown(
        """
        <style>
        .fecha-hoy { text-align: right; color: #f8fafc; font-size: 1.2rem; font-weight: 600; padding-top: 0.8rem; }
        .subtitulo { color: #94a3b8; font-size: 0.9rem; }
        [data-testid="stAppViewContainer"] { background-color: #0f172a; }
        [data-testid="stHeader"] { background-color: #0f172a; border-bottom: 1px solid #334155; }
        div[data-testid="stMetric"] { background-color: #111827; border: 1px solid #334155; border-radius: 10px; padding: 0.8rem; }
        div[data-testid="stMetricLabel"] { color: #94a3b8; text-transform: uppercase; font-size: 0.75rem; }
        div[data-testid="stMetricValue"] { color: #f8fafc; font-weight: 700; }
        div[data-testid="stVerticalBlockBorderWrapper"] { background-color: #111827; border: 1px solid #334155; border-radius: 10px; }
        [data-testid="stSubheader"] h3 { color: #f8fafc; }
        [data-testid="stCaptionContainer"] p { color: #cbd5e1; }
        </style>
        """,
        unsafe_allow_html=True,
    )

'''
old = '    store = start_consumer()\n    live_dashboard()'
new = css + '\n    store = start_consumer()\n    live_dashboard()'
if old in s and '<style>' not in s:
    with open(path, 'w', encoding='utf-8') as f:
        f.write(s.replace(old, new))
    print('done')
else:
    print('skip')
