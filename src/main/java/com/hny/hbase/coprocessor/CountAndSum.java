package com.hny.hbase.coprocessor;

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

public class CountAndSum extends CountAndSumProtocol.RowCountAndSumService implements Coprocessor, CoprocessorService {

    private RegionCoprocessorEnvironment env;



    @Override
    public void getCountAndSum(RpcController controller, CountAndSumProtocol.CountAndSumRequest request, RpcCallback<CountAndSumProtocol.CountAndSumResponse> done) {
        String family = request.getFamily();
        if (null == family || "".equals(family)) {
            throw new NullPointerException("you need specify the family");
        }
        String column = request.getColumn();
        if (null == column || "".equals(column))
            throw new NullPointerException("you need specify the column");


        //获取scan对象
        Scan scan = new Scan();

            scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));




        CountAndSumProtocol.CountAndSumResponse response = null;
        InternalScanner scanner = null;
        try {
            // 计数
            long count = 0;
            // 求和
            double sum = 0;

            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<Cell>();
            boolean hasMore ;
            // 切记不要用while(){}的方式，这种方式会丢失最后一条数据
            do {
                hasMore = scanner.next(results);
                if (results.isEmpty()) {
                    continue;
                }
                Cell kv = results.get(0);
                double value = 0;
                try {
                    value = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(kv)));
                } catch (Exception e) {
                }
                count++;
                sum += value;
                results.clear();
            } while (hasMore);

            // 生成response
            response = CountAndSumProtocol.CountAndSumResponse.newBuilder().setCount(count).setSum(sum).build();
        } catch (IOException e) {
            e.printStackTrace();
            ResponseConverter.setControllerException(controller, e);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {
                }
            }
        }
        done.run(response);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // do nothing
    }

    @Override
    public Service getService() {
        return this;
    }
}