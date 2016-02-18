package resa.optimize;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by Tom.fu on 18/2/2016.
 */
public class GeneralServiceModelTest {

    @Test
    public void testGetExpectedTotalSojournTime() throws Exception {
        GeneralServiceNode a = new GeneralServiceNode("a", 2, 1.0, 1.0, 1.0, 200.0, 5.0, 100, 10, 10.0, 15, 15, 2.0, 5, 5);
        GeneralServiceNode b = new GeneralServiceNode("b", 3, 1.0, 1.0, 1.0, 100.0, 5.0, 100, 10, 10.0, 25, 25, 2.0, 5, 5);

        Map<String, GeneralServiceNode> serviceNodes = new HashMap<>();
        serviceNodes.put(a.getComponentID(), a);
        serviceNodes.put(b.getComponentID(), b);

        Map<String, Integer> allocation = new HashMap<>();
        allocation.put(a.getComponentID(), a.getExecutorNumber());
        allocation.put(b.getComponentID(), b.getExecutorNumber());

        System.out.println(GeneralServiceModel.getExpectedTotalSojournTimeForJacksonOQN(serviceNodes, allocation));
        System.out.println(GeneralServiceModel.getExpectedTotalSojournTimeForGeneralizedOQN_SimpleAppr(serviceNodes, allocation));
        System.out.println(GeneralServiceModel.getExpectedTotalSojournTimeForGeneralizedOQN_ComplexAppr(serviceNodes, allocation));
    }


    @Test
    public void testSuggestAllocationGeneralTop() throws Exception {

        GeneralServiceNode a = new GeneralServiceNode("a", 2, 1.0, 1.0, 1.0, 100.0, 2.0, 100, 10, 10.0, 5, 5, 100.0, 5, 5);
        GeneralServiceNode b = new GeneralServiceNode("b", 3, 1.0, 1.0, 1.0, 120.0, 2.0, 100, 10, 10.0, 5, 5, 100.0, 5, 5);

        Map<String, GeneralServiceNode> serviceNodes = new HashMap<>();
        serviceNodes.put(a.getComponentID(), a);
        serviceNodes.put(b.getComponentID(), b);

        System.out.println(GeneralServiceModel.suggestAllocationGeneralTopApplyMMK(serviceNodes, 8));
        System.out.println(GeneralServiceModel.suggestAllocationGeneralTopApplyGGK_SimpleAppr(serviceNodes, 8));
        System.out.println(GeneralServiceModel.suggestAllocationGeneralTopApplyGGK_ComplexAppr(serviceNodes, 8));
    }

    @Test
    public void testCheckOptimized() throws Exception {

        GeneralSourceNode s = new GeneralSourceNode("s", 1, 1, 1, 1, 100.0, 1.0, 100, 10, 10, 5, 5, 1.0, 5, 5, 1.0);

        GeneralServiceNode a = new GeneralServiceNode("a", 2, 1.0, 1.0, 1.0, 100.0, 2.0, 100, 10, 10.0, 25, 25, 100.0, 5, 5);
        GeneralServiceNode b = new GeneralServiceNode("b", 3, 1.0, 1.0, 1.0, 120.0, 2.0, 100, 10, 10.0, 25, 25, 100.0, 5, 5);

        Map<String, GeneralServiceNode> serviceNodes = new HashMap<>();
        serviceNodes.put(a.getComponentID(), a);
        serviceNodes.put(b.getComponentID(), b);

        Map<String, Integer> allocation = new HashMap<>();
        allocation.put(a.getComponentID(), a.getExecutorNumber());
        allocation.put(b.getComponentID(), b.getExecutorNumber());

        System.out.println(GeneralServiceModel.checkOptimized(s, serviceNodes, 5000, allocation, 8, 5, GeneralServiceModel.ServiceModelType.MMK));
    }

    @Test
    public void testGetMinReqServerCount() throws Exception {
        System.out.println(GeneralServiceModel.getMinReqServerCount(5, 10));
        System.out.println(GeneralServiceModel.getMinReqServerCount(5, 5));
        System.out.println(GeneralServiceModel.getMinReqServerCount(5, 1));
    }

    @Test
    public void testSojournTime() throws Exception {
        double l = 5;
        double ca = 5;
        double cs = 2;
        double m = 10;
        int k = 2;
        System.out.println(GeneralServiceModel.sojournTime_MMK(5, 10, 2));
        System.out.println(GeneralServiceModel.sojournTime_GGK_SimpleAppr(5, 1, 10, 1, 2));
        System.out.println(GeneralServiceModel.sojournTime_GGK_SimpleAppr(5, 2, 10, 5, 2));
        System.out.println(GeneralServiceModel.sojournTime_GGK_SimpleAppr(5, 5, 10, 2, 2));
        System.out.println(GeneralServiceModel.sojournTime_GGK_ComplexAppr(5, 1, 10, 1, 2));
        System.out.println(GeneralServiceModel.sojournTime_GGK_ComplexAppr(5, 2, 10, 5, 2));
        System.out.println(GeneralServiceModel.sojournTime_GGK_ComplexAppr(5, 5, 10, 2, 2));


    }
}