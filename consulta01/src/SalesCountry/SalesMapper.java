package SalesCountry;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        // Saltar la cabecera
        if (key.get() == 0) return;

        String valueString = value.toString().trim();
        String[] data = valueString.split(";");

        // Necesitamos al menos 6 columnas (indice 0..5)
        if (data.length < 6) return;

        // Extraer los 2 primeros digitos del UBIGEO como codigo de departamento
        // Columna 1 = UBIGEO (ej: "010101" -> departamento "01")
        String ubigeo = data[1].trim();
        if (ubigeo.length() < 2) return;
        String departamento = ubigeo.substring(0, 2);

        // Columna 4 = JUNTOS - Hogares afiliados
        // Columna 5 = JUNTOS - Hogares abonados
        int afiliados = 0;
        int abonados  = 0;

        try {
            if (!data[4].trim().isEmpty()) afiliados = Integer.parseInt(data[4].trim());
        } catch (NumberFormatException e) { afiliados = 0; }

        try {
            if (!data[5].trim().isEmpty()) abonados = Integer.parseInt(data[5].trim());
        } catch (NumberFormatException e) { abonados = 0; }

        int total = afiliados + abonados;

        // Emitir: clave = codigo departamento, valor = suma afiliados + abonados
        output.collect(new Text(departamento), new IntWritable(total));
    }
}