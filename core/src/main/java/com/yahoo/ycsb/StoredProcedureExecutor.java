package com.yahoo.ycsb;

import java.util.List;

public interface StoredProcedureExecutor {
  /**
  * Execute a stored procedure by providing its name and parameters. 
  *
  * @param spName The name of the stored procedure
  * @param keys The list of keys that the stored procedure will access
  * @return The result of the operation.
  */
  public abstract Status execStoredProcedure(String spName, List<String> keys);
}
