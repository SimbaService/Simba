package com.necla.simba.client;

oneway interface ISCSClient {
  void newData(String table, int rows, int numDeletedRows);
  void syncConflict(String table, int rows);
  void subscribeDone();
  void ping();
}
