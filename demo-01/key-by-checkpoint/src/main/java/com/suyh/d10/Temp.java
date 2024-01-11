package com.suyh.d10;

/**
 * @author suyh
 * @since 2024-01-11
 */
public class Temp {
    public static void main(String[] args) {
        int recordsPerCheckpoint = 10;

        for (int value = 0; value < 12 * recordsPerCheckpoint; value++) {
            long idCount = value / recordsPerCheckpoint + 1;
            int idMode = 0;
            for (int i = 0; i < idCount; i++) {
                int fileCount = i / 5;
                if ((fileCount % 2) == 0) {
                    int stride = 1;
                    idMode += stride;
                } else {
                    int stride = -1;
                    if (i % 5 == 0) {
                        // 如果没有这个自增，将会出现的现象是： 1 2 3 4 5 4 3 2 1 0
                        // 显然我期望的最大的只有5 最小的也只应该有1 ，这个0 是异常情况。
                        // 所以添加一个自增，在第一次自减时将它的值提升1 个数值。
                        idMode++;
                    }
                    idMode += stride;
                }
                System.out.println("idMode: " + idMode);
            }
            assert idMode > 0;
            long idValue = value % idMode + 1;
            System.out.println("idCount: " + idCount + ", idMode: " + idMode + ", idValue: " + idValue);
        }
    }
}
