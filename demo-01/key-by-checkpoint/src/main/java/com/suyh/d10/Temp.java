package com.suyh.d10;

/**
 * @author suyh
 * @since 2024-01-11
 */
public class Temp {
    public static void main(String[] args) {
        demo02();
    }

    private static void demo02() {
        int recordsPerCheckpoint = 10;

        int idModeFirst = 1;
        int idModeLast = 5;
        for (int value = 0; value < 30 * recordsPerCheckpoint; value++) {
            long idCount = value / recordsPerCheckpoint + 1;

            // 需要取一个对最终值取模的基础数值，  x % idMode = ...
            int idMode = idModeFirst;
            for (int i = 0; i < idCount; i++) {
                // 如果没有判断，将会出现的现象是： 1 2 3 4 5 4 3 2 1 0
                // 显然我期望的最大的只有5 最小的也只应该有1 ，这个0 是异常情况。
                // 所以需要忽略每一个首次的自增与自减
                // 以期望达到效果：1 2 3 4 5   5 4 3 2 1   1 2 3 4 5   5 4 3 2 1 ...
                if (i % idModeLast != 0) {
                    int fileCount = i / idModeLast;
                    // 步长为1 则自增，步长为-1 则自减
                    int stride = (fileCount % 2) == 0 ? 1 : -1;
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
