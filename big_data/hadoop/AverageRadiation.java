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
// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageRadiation {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    
    //Crea el job y configura sus parametros
    Job job = new Job();
    job.setJarByClass(AverageRadiation.class);
    job.setJobName("Average radiation");

    //Se guardan las rutas obtenidas de la llamada al comando (Entrada y salida de datos)
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    //Se le indican las clases de las correspondientes funciones map y reduce
    job.setMapperClass(AverageRadiationMapper.class);
    job.setReducerClass(AverageRadiationReducer.class);

    //se le indican los tipos de salida
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
