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