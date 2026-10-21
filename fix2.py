path = r"C:\Users\dgoo2\MVP1_EventosYStreamingEnTiempoReal\train-missed-connection-mvp\src\dashboard_app.py"
with open(path, encoding="utf-8") as f:
    s = f.read()
si = s.find("    store = start_consumer()")
if si < 0:
    print("no")
else:
    before = s[:si].rstrip()
    if before.endswith("unsafe_allow_html=True,"):
        j = before.rfind("    st.markdown")
        if j >= 0:
            before2 = before[:j].rstrip()
            s2 = before2 + "\n\n    store = start_consumer()\n    live_dashboard()\n\n\nif __name__ == \"__main__\":\n    main()\n"
            with open(path, "w", encoding="utf-8") as f:
                f.write(s2)
            print("ok")
        else:
            print("no2")
    else:
        print("no3")
