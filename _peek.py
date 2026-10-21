path = r'src/dashboard_app.py'
with open(path, encoding='utf-8') as f:
    s = f.read()
i = s.find('st.markdown')
if i >= 0:
    print('FOUND')
