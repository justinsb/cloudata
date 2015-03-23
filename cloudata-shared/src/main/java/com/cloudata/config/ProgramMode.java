package com.cloudata.config;

public enum ProgramMode {
  Production, Development;

  public static boolean isDevelopment() {
    return get() == ProgramMode.Development;
  }

  private static final ProgramMode MODE = findMode();

  private static ProgramMode findMode() {
    String modeString = System.getenv().get("PROGRAM_MODE");
    if (modeString == null) {
      return ProgramMode.Production;
    }
    modeString = modeString.toLowerCase();
    for (ProgramMode mode : ProgramMode.values()) {
      if (mode.name().toLowerCase().equals(modeString)) {
        return mode;
      }
    }
    throw new IllegalStateException("Unknown program mode: " + modeString);
  }

  public static ProgramMode get() {
    return MODE;
  }
}
