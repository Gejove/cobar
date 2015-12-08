/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * (created at 2011-7-20)
 */
package com.alibaba.cobar.route.function;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.cobar.config.model.rule.RuleAlgorithm;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;

/**
 * 针对MySQL的auto_increment自增长字段进行分库的路由算法
 * @author <a href="mailto:liguiju@qudian-inc.com">Li Guiju</a>
 */
public final class PartitionBySerial extends PartitionFunction implements RuleAlgorithm {
    // 分区长度:数据段分布定义，其中取模的数一定要是2^n， 因为这里使用x % 2^n == x & (2^n - 1)等式，来优化性能。
    private static final int PARTITION_LENGTH = 1024;
    
    public PartitionBySerial(String functionName) {
        this(functionName, null);
    }

    public PartitionBySerial(String functionName, List<Expression> arguments) {
        super(functionName, arguments);
    }

    @Override
    public Object evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        return calculate(parameters)[0];
    }

    @Override
    public Integer[] calculate(Map<? extends Object, ? extends Object> parameters) {
        Integer[] rst = new Integer[1];
        Object arg = arguments.get(0).evaluation(parameters);
        if (arg == null) {
            throw new IllegalArgumentException("partition key is null ");
        } else if (arg == UNEVALUATABLE) {
            throw new IllegalArgumentException("argument is UNEVALUATABLE");
        }
        Number key;
        if (arg instanceof Number) {
            key = (Number) arg;
        } else if (arg instanceof String) {
            key = Long.parseLong((String) arg);
        } else {
            throw new IllegalArgumentException("unsupported data type for partition key: " + arg.getClass());
        }
        
        //根据分区数量count,分区长度length计算各分库的映射分区索引
        long modulus = key.longValue() & (count[0] -1);			 //使用x % 2^n == x & (2^n - 1)等式，来优化性能,%转换为&操作
        long groupPartionIndex = (modulus == 0) ? count[0] : modulus;
        long groupMappingIndex = (groupPartionIndex - 1 ) * length[0] + (PARTITION_LENGTH / 2 * count[0]);
        rst[0] = partitionIndex(groupMappingIndex);
        
        return rst;
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        if (arguments == null || arguments.size() != 1)
            throw new IllegalArgumentException("function " + getFunctionName() + " must have 1 argument but is "
                    + arguments);
        Object[] args = new Object[arguments.size()];
        int i = -1;
        for (Expression arg : arguments) {
            args[++i] = arg;
        }
        return (FunctionExpression) constructMe(args);
    }

    @Override
    public RuleAlgorithm constructMe(Object... objects) {
        List<Expression> args = new ArrayList<Expression>(objects.length);
        for (Object obj : objects) {
            args.add((Expression) obj);
        }
        PartitionBySerial partitionFunc = new PartitionBySerial(functionName, args);
        partitionFunc.count = count;
        partitionFunc.length = length;
        return partitionFunc;
    }

    @Override
    public void initialize() {
        init();
    }

}
