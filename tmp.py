import duckdb                                                                                                                                                       
con = duckdb.connect("test_gui.duckdb")
rows = con.execute("SELECT variable_name, version_keys FROM _record_metadata LIMIT 20").fetchall()                                                                    
for r in rows:
    print(r)                                                                                                                                                          
con.close() 