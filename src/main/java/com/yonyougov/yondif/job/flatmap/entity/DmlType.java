package com.yonyougov.yondif.job.flatmap.entity;

/**
 * @Author zxz
 * @description DML操作类型及转换方法
 * @date 2022年06月17日 9:19
 */
public enum DmlType {
    /**
     * 插入
    */
    INSERT,
    /**
     * 更新
     */
    UPDATE,
    /**
     * 删除
     */
    DELETE;

    private static DmlType[] values = null;
    public static DmlType fromInt(int i) {
        if(DmlType.values == null) {
            DmlType.values = DmlType.values();
        }
        return DmlType.values[i];
    }

    public static DmlType fromString(String type) {
        if(DmlType.values == null) {
            DmlType.values = DmlType.values();
        }
        if(type==null){
            return null;
        }
        switch (type.toLowerCase()){
            case "i":
                return INSERT;
            case "u":
                return UPDATE;
            //mysql cdc delete，也是d
            case "d":
                return DELETE;
            //mysql cdc type,update
            case "r":
                return UPDATE;
            //mysql cdc type,insert
            case "c":
                return INSERT;
            default:
                return null;
        }
    }
}
