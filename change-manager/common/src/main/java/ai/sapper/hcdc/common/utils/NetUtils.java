package ai.sapper.hcdc.common.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class NetUtils {
    public static List<InetAddress> getInetAddresses() throws Exception {
        List<InetAddress> addresses = new ArrayList<>();
        Enumeration e = NetworkInterface.getNetworkInterfaces();
        while (e.hasMoreElements()) {
            NetworkInterface n = (NetworkInterface) e.nextElement();
            Enumeration ee = n.getInetAddresses();
            while (ee.hasMoreElements()) {
                InetAddress i = (InetAddress) ee.nextElement();
                addresses.add(i);
            }
        }
        return addresses;
    }
}
