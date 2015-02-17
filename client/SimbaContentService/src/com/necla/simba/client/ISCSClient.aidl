package com.necla.simba.client;

oneway interface ISCSClient {
  void newData(String table, int numNewRows, int numDeletedRows);
  void syncConflict(String table, int rows);
  void subscribeDone();
  void ping();
}