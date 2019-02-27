package com.huawei.hive.udf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.util.JavaDataModel;

import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
参考连接：
https://blog.csdn.net/kent7306/article/details/50110067
*/





@Description(name = "wordcount", 
             value = "_FUNC_(expr) - Returns the count of expr"
					+        "rows containing NULL values.\n"
					+"_FUNC_(expr,true) - Returns the count of expr"
				    +        "rows containing NULL values.\n"
				    +"_FUNC_(expr,false) - Returns the count of expr"
					+        "rows not containing NULL values.\n"    )
//@SuppressWarnings("deprecation")
public class GenericUDAFWordcount2    extends AbstractGenericUDAFResolver{
	static final Logger LOG = LoggerFactory.getLogger(GenericUDAFWordcount2.class.getName());
	
	 @SuppressWarnings("deprecation")
	@Override
	 public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo) throws SemanticException {

		    TypeInfo[] parameters = paramInfo.getParameters();

		    if (parameters.length == 0) {
		      if (!paramInfo.isAllColumns()) {
		        throw new UDFArgumentException("Argument expected");
		      }
		      assert !paramInfo.isDistinct() : "DISTINCT not supported with *";
		    } else {
		      if (parameters.length > 2 && !paramInfo.isDistinct()) {
		        throw new UDFArgumentException("DISTINCT keyword must be specified");
		      }
		      assert !paramInfo.isAllColumns() : "* not supported in expression list";
		    }
            
		    //获取参数值
		    Boolean  countNull = true;
		    ObjectInspector countNullOI;
		    ObjectInspector[] paramOIs = paramInfo.getParameterObjectInspectors();
		    if( paramOIs.length > 1) {
		    	countNullOI  =   paramOIs[1];
		    	Object o = (  (ConstantObjectInspector) countNullOI  ).getWritableConstantValue();
		    	countNull = ( (BooleanWritable) o).get();
		    	
		    }
		    
       
		    GenericUDAFWordcountEvaluator genericUDAFWordcountEvaluator = new GenericUDAFWordcountEvaluator();
		    return genericUDAFWordcountEvaluator;
		  }	  
	 
	  public static class GenericUDAFWordcountEvaluator extends GenericUDAFEvaluator {
		  
		   // private boolean countAllColumns = false;
		   // private boolean countDistinct = false;
		    private boolean countNull = true;
		    private LongObjectInspector partialCountAggOI;
		    private ObjectInspector[]  outputOI;
		    protected transient PrimitiveObjectInspector inputOI1;
		    protected transient PrimitiveObjectInspector inputOI2;
		    private LongWritable result;
		    private boolean flag = false;
		    
		    
		    public void setCountNull(boolean countNull) {
		    	this.countNull = countNull;
		    }
		    
		 @Override
		    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
		    throws HiveException {
		      super.init(m, parameters);
		      Mode mode = m;
		      if (mode == Mode.PARTIAL2 || mode == Mode.FINAL) {
		        partialCountAggOI = (LongObjectInspector)parameters[0];
		      } else {
		        inputOI1 = (PrimitiveObjectInspector) parameters[0];
		        if( parameters.length > 1  ) inputOI2 = (PrimitiveObjectInspector) parameters[1];
		  //      outputOI = ObjectInspectorUtils.getStandardObjectInspector(inputOI, ObjectInspectorCopyOption.JAVA);
		      }
		      result = new LongWritable(0);
		      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
		    }
		 
		 
		    /** class for storing count value. */
		    @AggregationType(estimable = true)
		    static class CountAgg extends AbstractAggregationBuffer {
		      long value;
		      @Override
		      public int estimate() { return JavaDataModel.PRIMITIVES2; }
		    }
		    
		    @SuppressWarnings("deprecation")
			@Override
		    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
		      CountAgg buffer = new CountAgg();
		      reset(buffer);
		      return buffer;
		    }
		    
		    @SuppressWarnings("deprecation")
			@Override
		    public void reset(AggregationBuffer agg) throws HiveException {
		      ((CountAgg) agg).value = 0;
		    }
		    
		    
		    
		    @SuppressWarnings("deprecation")
			@Override
		    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
		      // parameters == null means the input table/split is empty
		      if (parameters == null) {
		        return;
		      }
		      
		      if(!flag) { 
		    	  flag = true;
		    	  countNull =( parameters.length > 1 ? PrimitiveObjectInspectorUtils.getBoolean(parameters[1], inputOI2) : countNull);
		      }

		      if (countNull) {
		          ((CountAgg) agg).value++;
		      }else {
		    	  if(parameters[0] != null) {
		    		  ((CountAgg) agg).value++;	    		  
		    	  }
		      }
		    }

		    @Override
		    public void merge(AggregationBuffer agg, Object partial)
		      throws HiveException {
		      if (partial != null) {
		        long p = partialCountAggOI.get(partial);
		        ((CountAgg) agg).value += p;
		      }
		    }		    
		    
		    @SuppressWarnings("deprecation")
			@Override
		    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
		      return terminate(agg);
		    } 
		 
		    @SuppressWarnings("deprecation")
			@Override
		    public Object terminate(AggregationBuffer agg) throws HiveException {
		      result.set(((CountAgg) agg).value);
		      return result;
		    }
	 }
	 

}
