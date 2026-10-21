import re
path = r'C:\Users\dgoo2\MVP1_EventosYStreamingEnTiempoReal\train-missed-connection-mvp\src\dashboard_app.py'
with open(path, encoding='utf-8') as f:
    s = f.read()
m = re.search(r'<style>.*?</style>', s, re.DOTALL)
print('FOUND' if m else 'NONE')
if m:
    print(m.start(), m.end())
