package com.shakti.flinkdemo.usecases.uc20_customio;

/** One line of a file, as emitted by {@link LinesSource}. */
public record FileLine(String fileName, long lineNo, String text) {}
