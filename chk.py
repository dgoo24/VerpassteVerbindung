s=open('src/dashboard_app.py',encoding='utf-8').read()
print(s.count('"""'), len(s.splitlines()))
