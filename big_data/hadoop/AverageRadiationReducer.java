/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bigdata;

/**
 *
 * @author alvaro
 */
// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageRadiationReducer
  extends Reducer<Text, IntWritable, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    
    int average_sum = 0;
    int counter = 0;
    
    for (IntWritable value : values) {
        average_sum += value.get();
        counter++;
    }

    //Sacamos la media 
    float average_radiation = (float)average_sum/counter;
    
    //Escribimos en la salida el nombre y el valor formateado con 2 decimales
    context.write(key, new Text(String.format("%.02f", average_radiation)));
  }
}

