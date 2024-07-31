package com.yonyougov.yondif.log;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

public class LogMarker {
    public static Marker trace_marker = MarkerManager.getMarker("trace_marker");
    public static Marker error_marker = MarkerManager.getMarker("error_marker");
}
