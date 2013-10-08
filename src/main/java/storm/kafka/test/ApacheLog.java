package storm.kafka.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Hello world!
 *
 */
public class ApacheLog 
{
	static final Logger logger = Logger.getLogger(ApacheLog.class);

	public static void main(String[] args)	{

		
		int aux=800;
		for(int i=aux;i<aux+20;i++) {
			String HOSTREMOTO="85.155.188.197";
			String NOMBRELOGREMOTO="-";
			String USUARIOREMOTO="user" + i;
			String TIEMPOEJECPETICION="[17/Sep/2012:19:01:24+0200]";
			String LINEAPETICION="\"GET_/Estatico/Globales/V114/Bhtcs/Internet/AT/\"";
			String ESTADOPETICION="200";
			String TAMAÑORESPUESTA="3117";
			String REFERER="\"-\"";
			String USERAGENT="\"Chrome/21.0.1180.89\"";
			String IDSESION="\"0000z2ur1hruUUG-MhpsITK9JY_:16vnisqka\"";
			String TIEMPORESPUESTA="1020";
			
			logger.info(
					 HOSTREMOTO + " " +
					 NOMBRELOGREMOTO + " " +
					 USUARIOREMOTO + " " +
					 TIEMPOEJECPETICION + " " +
					 LINEAPETICION + " " +
					 ESTADOPETICION + " " +
					 TAMAÑORESPUESTA + " " +
					 REFERER + " " +
					 USERAGENT + " " +
					 IDSESION + " " +
					 TIEMPORESPUESTA		
			);
		}
	
		
		
		
/*
 * LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" combined
 
Respecto al log único/unificado el formato del access es distinto
 
Configuración:
 
LogFormat "%h %l %u %t \"%r\" %>s %b \"%{User-Agent}i\" \"%{JSESSIONID}C\" %D" common
 
Ejemplos:
 85.155.188.197 - - [17/Sep/2012:19:01:24 +0200] 
 "GET /Estatico/Globales/V114/Bhtcs/Internet/AT/Plugins/Tabla/OrdenCol_mM.bjs HTTP/1.1" 200 3117 
 "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1" 
 "0000z2ur1hruUUG-MhpsITK9JY_:16vnisqka" 1020
 */
	}
}
