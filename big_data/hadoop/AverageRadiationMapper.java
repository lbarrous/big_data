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
// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageRadiationMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static final String[] estaciones_permitidas = {"2","3","4","5","6","7","8","9","10"};
  
  //Funcion que determina si el valor de radiacion recibido de la linea es correcto
  //(Tiene formato numerico)
  private boolean isCorrect(String radiacion) {
      return radiacion.matches("[-+]?\\d*\\.?\\d+");
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    
    //Cogemos cada linea y la troceamos con split a partir de cada ";"
    //de modo que conseguiremos meter en un array de strings cada parte de la linea
    String[] linea = line.split(";",-1);
    
    //Reemplazamos las comillas para hacer la comparacion con las estaciones validas
    String id_estacion = linea[2].replace("\"", "");
    String nombre_estacion = linea[3].replace("\"", "");
    
    if(Arrays.asList(estaciones_permitidas).contains(id_estacion)) {
        String radiacion = linea[16];
        
        if(isCorrect(radiacion)) {
            //Si la estacion es valida y el valor de radiacion es correcto lo escribimos
            int radiacion_entero = Integer.parseInt(radiacion);
            context.write(new Text(nombre_estacion), new IntWritable(radiacion_entero));
        }
    }
  }
}

