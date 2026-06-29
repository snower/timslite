package io.github.snower.timslite;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

final class KotlinConversions {
    private static final Method UBYTE_BOX;
    private static final Method UBYTE_UNBOX;
    private static final Method UINT_UNBOX;
    private static final Method ULONG_UNBOX;

    private static final ConcurrentHashMap<String, Method> METHOD_CACHE = new ConcurrentHashMap<>();

    // Kotlin mangles getter names for inline value class fields.
    // Suffixes observed with Kotlin 2.2.0:
    //   ULong: -s-VKNKU    UInt: -pVg5ArA    UShort: -Mh2AYeg    UByte: -w2LRezQ
    private static final String ULONG_SUFFIX = "-s-VKNKU";
    private static final String UINT_SUFFIX = "-pVg5ArA";
    private static final String USHORT_SUFFIX = "-Mh2AYeg";
    private static final String UBYTE_SUFFIX = "-w2LRezQ";

    static {
        try {
            UBYTE_BOX = kotlin.UByte.class.getDeclaredMethod("box-impl", byte.class);
            UBYTE_UNBOX = kotlin.UByte.class.getDeclaredMethod("unbox-impl");
            UINT_UNBOX = kotlin.UInt.class.getDeclaredMethod("unbox-impl");
            ULONG_UNBOX = kotlin.ULong.class.getDeclaredMethod("unbox-impl");
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to initialize Kotlin unsigned type conversions", e);
        }
    }

    private KotlinConversions() {
    }

    // ---- List<UByte> / byte[] conversions ----

    static List<kotlin.UByte> toUByteList(byte[] data) {
        List<kotlin.UByte> list = new ArrayList<>(data.length);
        for (byte b : data) {
            try {
                list.add((kotlin.UByte) UBYTE_BOX.invoke(null, b));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return list;
    }

    static byte[] fromUByteList(List<kotlin.UByte> list) {
        byte[] result = new byte[list.size()];
        for (int i = 0; i < result.length; i++) {
            try {
                result[i] = (byte) UBYTE_UNBOX.invoke(list.get(i));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }

    // ---- Mangled getter access ----

    static long getULong(Object obj, String baseName) {
        return (long) invokeMangledGetter(obj, baseName, ULONG_SUFFIX);
    }

    static int getUInt(Object obj, String baseName) {
        return (int) invokeMangledGetter(obj, baseName, UINT_SUFFIX);
    }

    static short getUShort(Object obj, String baseName) {
        return (short) invokeMangledGetter(obj, baseName, USHORT_SUFFIX);
    }

    static byte getUByte(Object obj, String baseName) {
        return (byte) invokeMangledGetter(obj, baseName, UBYTE_SUFFIX);
    }

    private static Object invokeMangledGetter(Object obj, String baseName, String suffix) {
        try {
            String key = obj.getClass().getName() + '#' + baseName + suffix;
            Method m = METHOD_CACHE.get(key);
            if (m == null) {
                m = findMangledMethod(obj.getClass(), baseName + suffix);
                METHOD_CACHE.put(key, m);
            }
            return m.invoke(obj);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e.getCause());
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static Method findMangledMethod(Class<?> clazz, String fullName) {
        for (Method m : clazz.getDeclaredMethods()) {
            if (m.getName().equals(fullName)) {
                m.setAccessible(true);
                return m;
            }
        }
        for (Class<?> iface : clazz.getInterfaces()) {
            for (Method m : iface.getDeclaredMethods()) {
                if (m.getName().equals(fullName)) {
                    return m;
                }
            }
        }
        throw new RuntimeException("Method not found: " + fullName + " in " + clazz.getName());
    }

    // ---- Bridge interface methods with inline value class types ----

    static Integer readLengthValue(Object bridge, long timestamp)
            throws io.github.snower.timslite.uniffi.TmslException {
        String key = "readLength-gbq4QnA#" + bridge.getClass().getName();
        try {
            Method m = METHOD_CACHE.get(key);
            if (m == null) {
                m = findInterfaceMethod(bridge.getClass(), "readLength-gbq4QnA", long.class);
                METHOD_CACHE.put(key, m);
            }
            kotlin.UInt result = (kotlin.UInt) m.invoke(bridge, timestamp);
            if (result == null) {
                return null;
            }
            return (int) UINT_UNBOX.invoke(result);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof io.github.snower.timslite.uniffi.TmslException) {
                throw (io.github.snower.timslite.uniffi.TmslException) cause;
            }
            throw new RuntimeException(cause);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    static io.github.snower.timslite.uniffi.DatasetBridge callOpenDatasetByIdentifier(
            Object bridge, long identifier)
            throws io.github.snower.timslite.uniffi.TmslException {
        String key = "openDatasetByIdentifier-VKZWuLQ#" + bridge.getClass().getName();
        try {
            Method m = METHOD_CACHE.get(key);
            if (m == null) {
                m = findInterfaceMethod(bridge.getClass(),
                    "openDatasetByIdentifier-VKZWuLQ", long.class);
                METHOD_CACHE.put(key, m);
            }
            return (io.github.snower.timslite.uniffi.DatasetBridge) m.invoke(bridge, identifier);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof io.github.snower.timslite.uniffi.TmslException) {
                throw (io.github.snower.timslite.uniffi.TmslException) cause;
            }
            throw new RuntimeException(cause);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static Method findInterfaceMethod(Class<?> implClass,
                                               String methodName,
                                               Class<?>... paramTypes) {
        for (Class<?> iface : implClass.getInterfaces()) {
            try {
                return iface.getDeclaredMethod(methodName, paramTypes);
            } catch (NoSuchMethodException ignored) {
            }
        }
        throw new RuntimeException(
            "Method not found: " + methodName + " in interfaces of " + implClass.getName());
    }
}
