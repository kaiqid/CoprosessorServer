package com.endpoint.test.max;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MaxServer extends maxProtocol.maxService implements Coprocessor,CoprocessorService{

    private RegionCoprocessorEnvironment env;   // 定义环境

    @Override
    public void getmax(RpcController controller, maxProtocol.maxRequest request, RpcCallback<maxProtocol.maxResponse> done) {
        String family = request.getFamily();
        if (null == family || "".equals(family)) {
            throw new NullPointerException("you need specify the family");
        }
        String column = request.getColumn();
        if (null == column || "".equals(column))
            throw new NullPointerException("you need specify the column");


        Scan scan = new Scan();

        // 设置扫描对象
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));


        // 定义变量
        maxProtocol.maxResponse response = null;
        InternalScanner scanner = null;

        // 扫描每个region，取值后求和
        try {
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<Cell>();
            boolean hasMore = false;
            Double max = null;
            do {
                hasMore = scanner.next(results);
                //for (Cell cell : results) {
                if (results.isEmpty()) {
                    continue;
                }
                Cell kv = results.get(0);
                try {
                    Double temp = Double.parseDouble(new String(CellUtil.cloneValue(kv)));
                    max = max != null && (temp == null || compare(temp, max) <= 0) ? max : temp;
                } catch (Exception e) {
                }


                results.clear();
                //sum += Long.parseLong(new String(CellUtil.cloneValue(cell)));

                //results.clear();
            } while (hasMore);
            // 设置返回结果
            response = maxProtocol.maxResponse.newBuilder().setMax((max!=null?max.doubleValue():Double.MAX_VALUE)).build();
        } catch (IOException e) {
            ResponseConverter.setControllerException(controller, e);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException e) {
                    //e.printStackTrace();
                }
            }
        }
        // 将rpc结果返回给客户端
        done.run(response);

    }



    public static int compare(Double l1, Double l2) {
        if (l1 == null ^ l2 == null) {
            return l1 == null ? -1 : 1; // either of one is null.
        } else if (l1 == null)
            return 0; // both are null
        return l1.compareTo(l2); // natural ordering.
    }
    @Override
    public Service getService() {

        return this;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)env;
        } else {
            throw new CoprocessorException("no load region");
        }

    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {

    }

    /**
     * <code>rpc getmax(.maxRequest) returns (.maxResponse);</code>
     *
     * @param controller
     * @param request
     * @param done
     */

}