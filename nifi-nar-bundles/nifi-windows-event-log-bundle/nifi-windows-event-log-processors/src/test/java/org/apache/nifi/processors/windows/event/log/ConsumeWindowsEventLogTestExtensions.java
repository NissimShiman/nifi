/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.windows.event.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.processors.windows.event.log.jna.EventSubscribeXmlRenderingCallback;
import org.apache.nifi.processors.windows.event.log.jna.WEvtApi;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.MockSessionFactory;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
//import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.Kernel32Util;
import com.sun.jna.platform.win32.W32Errors;
import com.sun.jna.platform.win32.WinError;
import com.sun.jna.platform.win32.WinNT;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

//@ExtendWith(JNAJUnitExtension.class)
@ExtendWith(MockitoExtension.class)
public class ConsumeWindowsEventLogTestExtensions {
    @Mock
    Kernel32 kernel32;

    @Mock
    WEvtApi wEvtApi;

 //   @Mock
    WinNT.HANDLE subscriptionHandle;

  //  @Mock
    Pointer subscriptionPointer;

    private ConsumeWindowsEventLog evtSubscribe;
    private TestRunner testRunner;

    private ClassLoader jnaMockClassloader;

    public static List<WinNT.HANDLE> mockEventHandles(WEvtApi wEvtApi, Kernel32 kernel32, List<String> eventXmls) {
        List<WinNT.HANDLE> eventHandles = new ArrayList<>();
        for (String eventXml : eventXmls) {
            WinNT.HANDLE eventHandle = mock(WinNT.HANDLE.class);
            when(wEvtApi.EvtRender(isNull(), eq(eventHandle), eq(WEvtApi.EvtRenderFlags.EVENT_XML),
                    anyInt(), any(Pointer.class), any(Pointer.class), any(Pointer.class))).thenAnswer(invocation -> {
                Object[] arguments = invocation.getArguments();
                Pointer bufferUsed = (Pointer) arguments[5];
                byte[] array = StandardCharsets.UTF_16LE.encode(eventXml).array();
                if (array.length > (int) arguments[3]) {
                    when(kernel32.GetLastError()).thenReturn(W32Errors.ERROR_INSUFFICIENT_BUFFER).thenReturn(W32Errors.ERROR_SUCCESS);
                } else {
                    ((Pointer) arguments[4]).write(0, array, 0, array.length);
                }
                bufferUsed.setInt(0, array.length);
                return false;
            });
            eventHandles.add(eventHandle);
        }
        return eventHandles;
    }

    @BeforeEach
    public void setup() throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {

  /*      ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        Map <String, Map<String,String>> map = this.getClassOverrideMap();
        for (String name : map.keySet()) {
      //  if (classOverrides != null) {
            System.out.println(name + " will be overridden");
            ClassPool classPool = ClassPool.getDefault();
            try {
                CtClass ctClass = classPool.get(name);
                try {
                    for (Map.Entry<String, String> methodAndBody : map.get(name).entrySet()) {
                        for (CtMethod loadLibrary : ctClass.getDeclaredMethods(methodAndBody.getKey())) {
                            loadLibrary.setBody(methodAndBody.getValue());
                        }
                    }

                    byte[] bytes = ctClass.toBytecode();
                    Class<?> definedClass = defineClass(name, bytes, 0, bytes.length);
                    if (resolve) {
                        resolveClass(definedClass);
                    }
                    System.out.println("definedClass: " + definedClass.getName());
                    return definedClass;
                } finally {
                    ctClass.detach();
                }
            } catch (Exception e) {
                throw new ClassNotFoundException(name, e);
            }
     //   }
    } */



 //       System.out.println("in setup() - system specific name: " + Kernel32Util.class.getCanonicalName());

//        JNAOverridingJUnitExtension.class.getClassLoader().loadClass("com.sun.jna.platform.win32.Kernel32Util");
  //      JNAOverridingJUnitExtension.class.getClassLoader().loadClass("com.sun.jna.Native");

 //       JNAOverridingJUnitExtension.class.getClassLoader().loadClass("com.sun.jna.platform.win32.Kernel32");

        String classpath = System.getProperty("java.class.path");
        URL[] result = Pattern.compile(File.pathSeparator).splitAsStream(classpath).map(Paths::get).map(Path::toAbsolutePath).map(Path::toUri)
                .map(uri -> {
                    URL url = null;
                    try {
                        url = uri.toURL();
                    } catch (MalformedURLException e) {
                        Assert.fail(String.format("Unable to create URL for classpath entry '%s'", uri));
                    }
                    return url;
                })
                .toArray(URL[]::new);


        Map<String, Map<String, String>> map = getClassOverrideMap();

        jnaMockClassloader = new URLClassLoader(result, null) {
            @Override
            protected
             synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                System.out.println("class being loaded in loadClass(): " + name);
                Map<String, String> classOverrides = map.get(name);
                if (classOverrides != null) {
                    System.out.println(name + " will be overridden");
                    ClassPool classPool = ClassPool.getDefault();
                    try {
                        CtClass ctClass = classPool.get(name);

                        CtClass interfaces [] = ctClass.getInterfaces();
                        for (CtClass c : interfaces) {
                            System.out.println("interface: " + c.getName());
                        }

                        try {
                            for (Map.Entry<String, String> methodAndBody : classOverrides.entrySet()) {
                                for (CtMethod loadLibrary : ctClass.getDeclaredMethods(methodAndBody.getKey())) {
                                    System.out.println("key :" + methodAndBody.getKey());
                                    loadLibrary.setBody(methodAndBody.getValue());
                                    System.out.println("value :" + methodAndBody.getValue());
                                }
                            }

                            byte[] bytes = ctClass.toBytecode();
                            Class<?> definedClass = defineClass(name, bytes, 0, bytes.length);
                            if (resolve) {
                                resolveClass(definedClass);
                            }
                            System.out.println(name + " has been defined and returned specially");
                            return definedClass;
                        } finally {
                            ctClass.detach();
                        }
                    } catch (Exception e) {
                        throw new ClassNotFoundException(name, e);
                    }
                } else if (name.startsWith("org.junit.") || name.startsWith("org.mockito")) {
                    System.out.println("special handling for junit/mockito class: " + name);
                    Class<?> result = ConsumeWindowsEventLogTestExtensions.class.getClassLoader().loadClass(name);
                    if (resolve) {
                        System.out.println("junit/mockito class " + name + " has been resolved");
                        resolveClass(result);
                    }
                    return result;
                }
                if (name.contentEquals("com.sun.jna.platform.win32.Kernel32")) {
                    System.out.println("Kernel32 found");
            //        return super.loadClass("com.sun.jna.platform.win32.WinError", resolve);
                }
                return super.loadClass(name, resolve);
            }
        };

    //    jnaMockClassloader.loadClass();
        for (String className : map.keySet())
        {
            Class<?> clazz = jnaMockClassloader.loadClass(className);
        }
        jnaMockClassloader.loadClass(MockitoExtension.class.getCanonicalName());
        jnaMockClassloader.loadClass(Mockito.class.getCanonicalName());
        Class<?> k32 =  jnaMockClassloader.loadClass(Kernel32.class.getCanonicalName());

   //     kernel32 = k32.newInstance();
        jnaMockClassloader.loadClass(ConsumeWindowsEventLogTestExtensions.class.getCanonicalName());




        /*

        CustomClassLoader loader = new CustomClassLoader(result, ConsumeWindowsEventLogTestExtensions.class.getClassLoader());
        loader.loadClass(ConsumeWindowsEventLogTestExtensions.class.getCanonicalName());
        Map <String, Map<String, String>> map = loader.getClassOverrideMap();

        for (String className : map.keySet())
        {
            loader.loadClass(className, false);
        }
*/
 //       jnaMockClassloader.loadClass(MockitoJUnitRunner.class.getCanonicalName()).getConstructor(Class.class)
 //       .newInstance(jnaMockClassloader.loadClass(klass.getCanonicalName()));





        kernel32 = Mockito.mock(Kernel32.class);
    //    @Mock
         wEvtApi = Mockito.mock(WEvtApi.class);;

    //    @Mock
        subscriptionHandle = Mockito.mock(WinNT.HANDLE.class);

    //    @Mock
        subscriptionPointer = Mockito.mock(Pointer.class);;


        evtSubscribe = new ConsumeWindowsEventLog(wEvtApi, kernel32);

        Mockito.lenient().when(subscriptionHandle.getPointer()).thenReturn(subscriptionPointer);

        Mockito.lenient().when(wEvtApi.EvtSubscribe(isNull(), isNull(), eq(ConsumeWindowsEventLog.DEFAULT_CHANNEL), eq(ConsumeWindowsEventLog.DEFAULT_XPATH),
                isNull(), isNull(), isA(EventSubscribeXmlRenderingCallback.class),
                eq(WEvtApi.EvtSubscribeFlags.SUBSCRIBE_TO_FUTURE | WEvtApi.EvtSubscribeFlags.EVT_SUBSCRIBE_STRICT)))
                .thenReturn(subscriptionHandle);

        testRunner = TestRunners.newTestRunner(evtSubscribe);
    }

    @Test//(timeout = 10 * 1000)
    public void testProcessesBlockedEvents() throws UnsupportedEncodingException, ClassNotFoundException {
        Map<String, Map<String, String>> map = getClassOverrideMap();
/*        for (String className : map.keySet())
        {
            jnaMockClassloader.loadClass(className);
        }

*/
        testRunner.setProperty(ConsumeWindowsEventLog.MAX_EVENT_QUEUE_SIZE, "1");
        testRunner.run(1, false, true);
        EventSubscribeXmlRenderingCallback renderingCallback = getRenderingCallback();

        List<String> eventXmls = Arrays.asList("one", "two", "three", "four", "five", "six");
        List<WinNT.HANDLE> eventHandles = mockEventHandles(wEvtApi, kernel32, eventXmls);
        AtomicBoolean done = new AtomicBoolean(false);
        new Thread(() -> {
            for (WinNT.HANDLE eventHandle : eventHandles) {
                renderingCallback.onEvent(WEvtApi.EvtSubscribeNotifyAction.DELIVER, null, eventHandle);
            }
            done.set(true);
        }).start();

        // Wait until the thread has really started
        while (testRunner.getFlowFilesForRelationship(ConsumeWindowsEventLog.REL_SUCCESS).size() == 0) {
            testRunner.run(1, false, false);
        }

        // Process rest of events
        while (!done.get()) {
            testRunner.run(1, false, false);
        }

        testRunner.run(1, true, false);

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(ConsumeWindowsEventLog.REL_SUCCESS);
        assertEquals(eventXmls.size(), flowFilesForRelationship.size());
        for (int i = 0; i < eventXmls.size(); i++) {
            flowFilesForRelationship.get(i).assertContentEquals(eventXmls.get(i));
        }
    }

    @Test
    public void testStopProcessesQueue() throws InvocationTargetException, IllegalAccessException {
        testRunner.run(1, false);

        List<String> eventXmls = Arrays.asList("one", "two", "three");
        for (WinNT.HANDLE eventHandle : mockEventHandles(wEvtApi, kernel32, eventXmls)) {
            getRenderingCallback().onEvent(WEvtApi.EvtSubscribeNotifyAction.DELIVER, null, eventHandle);
        }

        ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, evtSubscribe, testRunner.getProcessContext());

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(ConsumeWindowsEventLog.REL_SUCCESS);
        assertEquals(eventXmls.size(), flowFilesForRelationship.size());
        for (int i = 0; i < eventXmls.size(); i++) {
            flowFilesForRelationship.get(i).assertContentEquals(eventXmls.get(i));
        }
    }

    @Test
    public void testScheduleErrorThenTriggerSubscribe() throws InvocationTargetException, IllegalAccessException {
        evtSubscribe = new ConsumeWindowsEventLog(wEvtApi, kernel32);


        when(subscriptionHandle.getPointer()).thenReturn(subscriptionPointer);

        when(wEvtApi.EvtSubscribe(isNull(), isNull(), eq(ConsumeWindowsEventLog.DEFAULT_CHANNEL), eq(ConsumeWindowsEventLog.DEFAULT_XPATH),
                isNull(), isNull(), isA(EventSubscribeXmlRenderingCallback.class),
                eq(WEvtApi.EvtSubscribeFlags.SUBSCRIBE_TO_FUTURE | WEvtApi.EvtSubscribeFlags.EVT_SUBSCRIBE_STRICT)))
                .thenReturn(null).thenReturn(subscriptionHandle);

        testRunner = TestRunners.newTestRunner(evtSubscribe);


        testRunner.run(1, false, true);

        WinNT.HANDLE handle = mockEventHandles(wEvtApi, kernel32, Arrays.asList("test")).get(0);
        List<EventSubscribeXmlRenderingCallback> renderingCallbacks = getRenderingCallbacks(2);
        EventSubscribeXmlRenderingCallback subscribeRenderingCallback = renderingCallbacks.get(0);
        EventSubscribeXmlRenderingCallback renderingCallback = renderingCallbacks.get(1);
        renderingCallback.onEvent(WEvtApi.EvtSubscribeNotifyAction.DELIVER, null, handle);

        testRunner.run(1, true, false);

        assertNotEquals(subscribeRenderingCallback, renderingCallback);
        verify(wEvtApi).EvtClose(subscriptionHandle);
    }

    @Test
    public void testScheduleError() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        evtSubscribe = new ConsumeWindowsEventLog(wEvtApi, kernel32);

        when(wEvtApi.EvtSubscribe(isNull(), isNull(), eq(ConsumeWindowsEventLog.DEFAULT_CHANNEL), eq(ConsumeWindowsEventLog.DEFAULT_XPATH),
                isNull(), isNull(), isA(EventSubscribeXmlRenderingCallback.class),
                eq(WEvtApi.EvtSubscribeFlags.SUBSCRIBE_TO_FUTURE | WEvtApi.EvtSubscribeFlags.EVT_SUBSCRIBE_STRICT)))
                .thenReturn(null);

        when(kernel32.GetLastError()).thenReturn(WinError.ERROR_ACCESS_DENIED);

        testRunner = TestRunners.newTestRunner(evtSubscribe);

        testRunner.run(1);
        assertEquals(0, getCreatedSessions(testRunner).size());
        verify(wEvtApi, never()).EvtClose(any(WinNT.HANDLE.class));
    }

    @Test
    public void testStopClosesHandle() {
        testRunner.run(1);
        verify(wEvtApi).EvtClose(subscriptionHandle);
    }

    @Test//(expected = ProcessException.class)
    public void testScheduleQueueStopThrowsException() throws Throwable {
        ReflectionUtils.invokeMethodsWithAnnotation(OnScheduled.class, evtSubscribe, testRunner.getProcessContext());

        WinNT.HANDLE handle = mockEventHandles(wEvtApi, kernel32, Arrays.asList("test")).get(0);
        getRenderingCallback().onEvent(WEvtApi.EvtSubscribeNotifyAction.DELIVER, null, handle);

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, evtSubscribe, testRunner.getProcessContext());
        } catch (InvocationTargetException e) {
            Assertions.assertEquals("Stopping the processor but there is no ProcessSessionFactory stored and there are messages in the internal queue. Removing the processor now will " +
                        "clear the queue but will result in DATA LOSS. This is normally due to starting the processor, receiving events and stopping before the onTrigger happens. The messages " +
                        "in the internal queue cannot finish processing until until the processor is triggered to run.", e.getCause().getMessage());
            //throw e.getCause();
        }
    }

    public EventSubscribeXmlRenderingCallback getRenderingCallback() {
        return getRenderingCallbacks(1).get(0);
    }

    public List<EventSubscribeXmlRenderingCallback> getRenderingCallbacks(int times) {
        ArgumentCaptor<EventSubscribeXmlRenderingCallback> callbackArgumentCaptor = ArgumentCaptor.forClass(EventSubscribeXmlRenderingCallback.class);
        verify(wEvtApi, times(times)).EvtSubscribe(isNull(), isNull(), eq(ConsumeWindowsEventLog.DEFAULT_CHANNEL), eq(ConsumeWindowsEventLog.DEFAULT_XPATH),
                isNull(), isNull(), callbackArgumentCaptor.capture(),
                eq(WEvtApi.EvtSubscribeFlags.SUBSCRIBE_TO_FUTURE | WEvtApi.EvtSubscribeFlags.EVT_SUBSCRIBE_STRICT));
        return callbackArgumentCaptor.getAllValues();
    }

    @Test
    public void testGetSupportedPropertyDescriptors() {
        assertEquals(ConsumeWindowsEventLog.PROPERTY_DESCRIPTORS, evtSubscribe.getSupportedPropertyDescriptors());
    }

    @Test
    public void testGetRelationships() {
        assertEquals(ConsumeWindowsEventLog.RELATIONSHIPS, evtSubscribe.getRelationships());
    }

    private static Set<MockProcessSession> getCreatedSessions(TestRunner testRunner) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        MockSessionFactory processSessionFactory = (MockSessionFactory) testRunner.getProcessSessionFactory();
        Method getCreatedSessions = processSessionFactory.getClass().getDeclaredMethod("getCreatedSessions");
        getCreatedSessions.setAccessible(true);
        return (Set<MockProcessSession>) getCreatedSessions.invoke(processSessionFactory);
    }

    public static final String TEST_COMPUTER_NAME = "testComputerName";
    public static final String KERNEL_32_UTIL_CANONICAL_NAME = Kernel32Util.class.getCanonicalName();

    public static final String NATIVE_CANONICAL_NAME = Native.class.getCanonicalName();
    public static final String LOAD_LIBRARY = "loadLibrary";


    protected Map<String, Map<String, String>> getClassOverrideMap() {
        Map<String, Map<String, String>> classOverrideMap = new HashMap<>();

        Map<String, String> nativeOverrideMap = new HashMap<>();
        nativeOverrideMap.put(LOAD_LIBRARY, "return null;");
        classOverrideMap.put(NATIVE_CANONICAL_NAME, nativeOverrideMap);

        Map<String, String> kernel32UtilMap = new HashMap<>();
        kernel32UtilMap.put("getComputerName", "return \"" + TEST_COMPUTER_NAME + "\";");
        classOverrideMap.put(KERNEL_32_UTIL_CANONICAL_NAME, kernel32UtilMap);

        return classOverrideMap;
    }

}
