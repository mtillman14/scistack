import duckdb                                                                                                                                                         
con = duckdb.connect("/home/mtillman/SciDB/test_gui.duckdb", read_only=True)                                                                                        
# Check ALL record_metadata rows, including version_keys content                                                                                                      
rows = con.sql("SELECT * FROM _record_metadata").fetchdf()                                                                                                            
print(rows.to_string())                                                                                                                                               
print(f"\nTotal rows: {len(rows)}")                                                                                                                                   
# Also check if version_keys really have content                                                                                                                      
print("\nversion_keys values:")                                                                                                                                       
for vk in rows['version_keys']:                                                                                                                                       
    print(f"  {repr(vk)}")                                                                                                                                            
con.close()