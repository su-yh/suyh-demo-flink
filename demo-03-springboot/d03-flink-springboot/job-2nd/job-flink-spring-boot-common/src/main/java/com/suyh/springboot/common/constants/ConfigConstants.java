package com.suyh.springboot.common.constants;

/**
 * @author suyh
 * @since 2024-01-17
 */
public class ConfigConstants {
    public static final String CDS_PROP_PREFIX_BASE = "cds.flink";
    public static final String CDS_PROP_PREFIX_COMMON = CDS_PROP_PREFIX_BASE + ".common";
    public static final String CDS_PROP_PREFIX_BATCH = CDS_PROP_PREFIX_BASE + ".batch";
    public static final String CDS_PROP_PREFIX_STREAM = CDS_PROP_PREFIX_BASE + ".stream";

    public static final String JOB_TYPE_KEY = CDS_PROP_PREFIX_COMMON + ".job-type";

    public static final String JOB_TYPE_BATCH = "batch";
    public static final String JOB_TYPE_STREAM = "stream";

}
