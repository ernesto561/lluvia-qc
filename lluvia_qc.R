
library(shiny)

#############Descarga_lluvia_mod.R#############
#Este script descarga los datos de lluvia de estaciones automáticas del MARN,
#convierte a lluvia horaria y al formato usado por el programa THRESH.
#La parte de la descarga de datos de lluvia está basado en su mayoría en un
#script creado por Miguel Alvarenga, de la Gerencia de Hidrología, usado para
#la verificación de los datos de las estaciones hidrométricas.

start_time <- Sys.time()

library(XML)
library(httr)
library(rvest)
library(tidyverse)
library(magrittr)
library(chron)
library(zoo)
library(plotly)
library(hablar)
library(DT)

Sys.setenv(TZ='America/El_Salvador')

#Archivo fuente de tabla relacional id
#Aquí van los datos de las estaciones que se quieren descargar
#En min act va el paso de tiempo que viene en la última transmisión
#Por ejemplo, Picacho actualiza a los 40 minutos de cada hora, pero en esa actualización
#el último dato es de los 20 minutos de cada hora
sourceref <- read.csv("./datos_estaciones/datos_estaciones_test2.csv", fileEncoding="latin1")
sourceref <- sourceref[which(sourceref$Estado != "Not Working"), ]
#Lista de id de estaciones
idlist <- as.vector(sourceref$Estacion.Id)

#fechas de consulta (7 días hacia atrás incluyendo fecha actual)
fecha1 <- Sys.Date() - 7
fecha2 <- Sys.Date()
#fecha1 = "2024-07-15"
#fecha2 = "2024-07-16"
mi.dates <- seq(fecha1, fecha2, "days")

#Nombres de meses a reemplazar
wrn.month <- c("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec")
gd.month <- c(as.character(1:12))

#Dataframe con los datos de todas las estaciones para los 7 días anteriores.
lluvia_7_dias <- data.frame(Estacion = character(), est_id = numeric(), fecha = as.POSIXct(character()), medicion = numeric())

#Loop de repaso por cada id (cada estación)
for (i in 1:length(idlist)) {
  print(idlist[i])
  currloc <- as.vector(sourceref[which(sourceref$Estacion.Id == idlist[i]), ][[3]])[1] #Estación actualmente examinada
  thresh_id <- as.vector(sourceref[which(sourceref$Estacion.Id == idlist[i]), ][[8]])[1] #Estación actualmente examinada
  time_step <- as.vector(sourceref[which(sourceref$Estacion.Id == idlist[i]), ][[6]])[1] # Paso de tiempo en minutos
  
  # Generar secuencia completa de tiempos
  time_seq <- seq.POSIXt(from = as.POSIXct(paste0(fecha1, " 00:00"), tz = "America/El_Salvador"), 
                         to = as.POSIXct(floor_date(Sys.time(), "10 min"), tz = "America/El_Salvador"), 
                         by = paste(time_step, "mins"))
  time_seq <- data.frame(fecha = time_seq)
  # Añadir columnas vacías para Estacion, est_id, id_thresh y medicion
  time_seq$Estacion <- currloc
  time_seq$est_id <- idlist[i]
  time_seq$id_thresh <- thresh_id
  time_seq$medicion <- NA
  
  #data frame repositorio inicialmente sin datos
  sumdf <- data.frame(Dia = character(), Hora = character(), medicion = character())
  
  #Loop de repaso por cada una de las fechas. En parametroid se selecciona el parametro a descargar. PC es precipitación acumulada.
  for (j in 1:length(mi.dates)) {
    print(mi.dates[j])
    theurl <- paste0("http://www.snet.gob.sv/Geologia/pcbase2/tabla2.php?estacionid=", idlist[i], "&parametroid=PC&fecha=", mi.dates[j])
    doc <- try(htmlParse(theurl), silent = TRUE)
    if (inherits(doc, "try-error")) {
      next
    }
    table <- try(readHTMLTable(doc, trim = TRUE, as.data.frame = TRUE, header = TRUE), silent = TRUE)
    if (inherits(table, "try-error") || is.null(table) || length(table) < 4 || is.null(table[[4]]) || nrow(table[[4]]) == 0) {
      print("no hay datos")
      next
    }
    # Lectura de estación
    mi.idest <- as.data.frame(table[3])
    mi.idest <- colnames(mi.idest)
    mi.idest <- mi.idest[2]
    mi.idest <- gsub("NULL.", "", mi.idest)
    # Lectura de tabla
    mi.table <- as.data.frame(table[4])
    # Eliminación de caracteres no útiles
    mi.table$NULL.Dia <- gsub(" &nbsp", "", mi.table$NULL.Dia)
    mi.table$NULL.Hora <- gsub(" &nbsp", "", mi.table$NULL.Hora)
    mi.table$NULL.Medicion <- gsub("&nbsp", "", mi.table$NULL.Medicion)
    # Cambio de nombre de columnas
    colnames(mi.table) <- c("Dia", "Hora", "medicion")
    # Combinación de tablas
    sumdf <- bind_rows(sumdf, mi.table)
  }
  
  # Verificar si sumdf tiene filas antes de proceder
  if (nrow(sumdf) > 0) {
    # Loop cambiando nombres de meses
    for (k in 1:12) {
      sumdf$Dia <- gsub(wrn.month[k], gd.month[k], sumdf$Dia)
    }
    # Cambio de columna dia en sumdf a formato fecha
    sumdf$fecha <- strptime(paste0(sumdf$Dia, " ", sumdf$Hora), format = "%m-%d-%Y %H:%M", tz = "America/El_Salvador")
    sumdf$medicion <- as.numeric(sumdf$medicion)
    sumdf$Estacion <- currloc
    sumdf$est_id <- idlist[i]
    sumdf$id_thresh <- thresh_id
    
    sumdf <- sumdf %>% 
      dplyr::select(Estacion, est_id, id_thresh, fecha, medicion)
    
    # Completar los pasos de tiempo faltantes
    time_seq <- full_join(time_seq, sumdf, by = "fecha")
    time_seq$Estacion <- ifelse(is.na(time_seq$Estacion.x), time_seq$Estacion.y, time_seq$Estacion.x)
    time_seq$est_id <- ifelse(is.na(time_seq$est_id.x), time_seq$est_id.y, time_seq$est_id.x)
    time_seq$id_thresh <- ifelse(is.na(time_seq$id_thresh.x), time_seq$id_thresh.y, time_seq$id_thresh.x)
    time_seq$medicion <- ifelse(is.na(time_seq$medicion.y), time_seq$medicion.x, time_seq$medicion.y)
    time_seq <- time_seq %>% dplyr::select(Estacion, est_id, id_thresh, fecha, medicion)
    
    lluvia_7_dias <- bind_rows(lluvia_7_dias, time_seq)
  } else {
    lluvia_7_dias <- bind_rows(lluvia_7_dias, time_seq)
  }
}

timestep <- sourceref %>% dplyr::select(timestep, Estacion)
lluvia_7_dias <- left_join(lluvia_7_dias, timestep)

#Eliminacion de datos random
lluvia_7_dias <- lluvia_7_dias %>% dplyr::filter(minute(fecha)%%timestep==0)

#Completar pasos de tiempo para cada estación
lluvia_7_dias_com_func <- function(est, ts){lluvia_7_dias %>% dplyr::filter(Estacion==est) %>%
    tidyr::complete(Estacion = est, est_id = est_id, id_thresh = id_thresh, fecha = seq(min(fecha, na.rm = TRUE), max(fecha, na.rm = TRUE), by = ts))}

lluvia_7_dias_com <- pmap(list(sourceref$Estacion, paste0(sourceref$timestep," ", "min")), lluvia_7_dias_com_func) %>%
  bind_rows()

#Agregar columna de hora (fecha_hora). Por algún motivo hay problemas cuando se hace una vez se ha creado un tibble
#usando group_by, dado que solo muestra la fecha sin hora para los que tienen las 00 horas.
lluvia_7_dias_com %<>% mutate(fecha_hora = droplevels(cut(fecha, breaks="hour")))

#Asignar formato de hora a nueva columna
lluvia_7_dias_com %<>% mutate(fecha_hora = lubridate::ymd_hms(fecha_hora, truncated = 3))


##############################################################################
##Corrección de datos de lluvia. Regla 1######################################
##############################################################################
#Regla 1: Eliminación de valores negativos en lluvia acumulada
lluvia_7_dias_com %<>% mutate(medicion = replace(medicion, medicion < 0, NA))

#Conversión de lluvia acumulada a lluvia instantánea. Debido a que parece ser que hay problemas con los datos de lluvia instantánea,
#se usarán lo de lluvia acumulada y se convertirán a instantánea.
#Se agrupan (group_by) debido a que es un solo df con todas las estaciones, para que la conversión a instantánea sea por estación.
lluvia_7_dias_inst <- group_by(lluvia_7_dias_com, Estacion) %>% # agrupa por la columna de estacion
  mutate(lluvia_inst = medicion - lag(medicion, default = medicion[1])) %>%
  ungroup()

##############################################################################
##Corrección de datos de lluvia. Reglas 2 a 4#################################
##############################################################################

#Valor máximo entre pasos de tiempo (mm)
max_inst<- 40

#Regla 2: Lluvia acumulada en los que la lluvia instantánea sea mayor que un umbral
lluvia_7_dias_inst %<>% group_by(Estacion) %>%
  mutate(medicion_corr = replace(medicion, (abs(lluvia_inst)>=max_inst & (lag(lluvia_inst)>=0 | is.na(lag(lluvia_inst)))), NA)) %>%
  ungroup()


#Regla 3: Lluvia acumulada en los que el valor siguiente sea menor que el anterior (lluvia instantánea negativa)

#Función cummax que toma en cuenta NA
cummax_na = function(x, na.rm = TRUE) {
  if(!na.rm) return(cummax(x))
  if(all(is.na(x))) return(x)
  # check for leading NAs to keep
  first_non_na = match(TRUE, !is.na(x))
  x = dplyr::coalesce(x, -Inf)
  result = cummax(x)
  if(first_non_na > 1) result[1:(first_non_na - 1)] = NA
  result
}

#Flag1 asigna TRUE si un valor es menor que todos los anteriores
lluvia_7_dias_inst %<>% group_by(Estacion) %>% 
  mutate(flag1 = ifelse(medicion_corr >= cummax_na(medicion_corr), FALSE, TRUE)) %>%
  ungroup() 

#Se identifican los valores en los que hay más de 3 valores menores que los anteriores de forma consecutiva
#https://stackoverflow.com/a/71731518/4268720
#Flag2 asigna TRUE si hay más de 3 valores consecutivos que sean menores que los anteriores

flag2 <- function(est){df <-lluvia_7_dias_inst %>% dplyr::filter(Estacion==est)
r <- rle(df$flag1)
r$values <- r$lengths >= 3 & r$values

df$flag2 <- inverse.rle(r)
return(df)  
}

lluvia_7_dias_inst_f2 <- map(unique(lluvia_7_dias_inst$Estacion), flag2) %>%
  bind_rows()

#se eliminan los valores menores que los anteriores, sin tomar en cuenta la regla 4
lluvia_7_dias_inst_f2 %<>% mutate(medicion_corr2 = replace(medicion_corr, flag1==T & flag2==F, NA))

#Regla 4: Lluvia acumulada en el que la que el valor siguiente sea menor que el anterior y se mantenga constante
lluvia_7_dias_inst_f2 %<>% mutate(medicion_corr3 = medicion_corr2)

value_corr <- function(est){df <-lluvia_7_dias_inst_f2 %>% dplyr::filter(Estacion==est)

#Se obtienen las posiciones de los primeros valores de cada grupo que tiene flag2=T
flag_pos <- df |>
  group_by(consecutive_id(flag2)) |>
  mutate(
    index = ifelse(flag2 == TRUE & row_number() == 1, TRUE, FALSE)
  ) |>
  ungroup() |>
  pull(index)

true_indices <- which(flag_pos == TRUE)

# If there is at least one TRUE flag
if (length(true_indices) > 0) {
  for (i in seq_along(true_indices)) {
    true_index <- true_indices[i]
    if (true_index > 1) {
      # Find the difference between the current TRUE index value and the previous row's value
      difference <- abs(df$medicion_corr2[true_index] - df$medicion_corr2[true_index - 1])
      if (is.na(difference)) {
        difference = 0
      }
      # Add this difference to all subsequent rows
      df$medicion_corr3[true_index:nrow(df)] <- df$medicion_corr3[true_index:nrow(df)]+difference
    }
  }
}

return(df)

}

lluvia_7_dias_inst <- map(unique(lluvia_7_dias_inst_f2$Estacion), value_corr) %>%
  bind_rows()

#Se corre una regla similar la regla 2
#Regla 2: Lluvia acumulada en los que la lluvia instantánea sea negativa
lluvia_7_dias_inst %<>% group_by(Estacion) %>%
  mutate(medicion_corr3 = replace(medicion_corr3, (lluvia_inst<0 & (lag(lluvia_inst)>=0 | is.na(lag(lluvia_inst)))), NA)) %>%
  ungroup()

#Flag 4: Para el caso en que los valores negativos son constantes, se vuelve a correr la bandera 1,
#Que identifica valores negativos menores que los anteriores
lluvia_7_dias_inst %<>% group_by(Estacion) %>% 
  mutate(flag4 = ifelse(medicion_corr3 >= cummax_na(medicion_corr3), FALSE, TRUE)) %>%
  ungroup() 

#La flag 5 es igual que la bandera 2
flag5 <- function(est){df <-lluvia_7_dias_inst %>% dplyr::filter(Estacion==est)
r <- rle(df$flag4)
r$values <- r$lengths >= 3 & r$values

#Flag2 asigna TRUE si hay más de 3 valores consecutivos que sean menores que los anteriores
df$flag5 <- inverse.rle(r)
return(df)  
}

lluvia_7_dias_inst_f2 <- map(unique(lluvia_7_dias_inst$Estacion), flag5) %>%
  bind_rows()

#se eliminan los valores menores que los anteriores, sin tomar en cuenta la regla 4
lluvia_7_dias_inst <- lluvia_7_dias_inst_f2 %>% mutate(medicion_corr4 = replace(medicion_corr3, flag4==T & flag5==F, NA))

#Interpolación solamente para datos con 1 NA
lluvia_7_dias_inst %<>% group_by(Estacion) %>%
  mutate(medicion_interp = zoo::na.approx(medicion_corr4, na.rm = FALSE, maxgap=1)) %>%
  ungroup()

lluvia_7_dias_inst <- group_by(lluvia_7_dias_inst, Estacion) %>% # agrupa por la columna de estacion
  mutate(lluvia_inst_corr = medicion_interp - lag(medicion_interp, default = medicion_interp[1]),
         error = medicion - medicion_interp) %>%
  ungroup()

est_error <- group_by(lluvia_7_dias_inst, Estacion) %>%
  dplyr::filter(any(error!=0 | all(is.na(error)))) 

perc_per <- group_by(lluvia_7_dias_inst, Estacion) %>%
  dplyr::summarize(nas = sum(is.na(medicion_interp)), 
                   total = n()) %>%
  ungroup() %>%
  mutate(perc_per = round(nas/total*100, 1)) %>%
  arrange(desc(perc_per))

vars1 <- sourceref$Estacion
vars2 <- unique(est_error$Estacion)

data_plot <-lluvia_7_dias_inst %>% 
  pivot_longer(cols = c(medicion, medicion_interp), names_to = "qc_lluvia_acum", values_to = "valores")

# Define UI for application 
ui <-fluidPage(
  
  # Application title
  titlePanel("Control de calidad de lluvia"),
  
  # Sidebar with a slider input 
  sidebarLayout(
    sidebarPanel(
      checkboxInput("checkbox", "Mostrar solo estaciones con errores", FALSE),
      selectizeInput('est', 'Estación', choices= vars1, selected = NULL, multiple = FALSE,
                     options = NULL),
      downloadButton("download", label="Descarga de datos")
    ),
    mainPanel(
      tabsetPanel(
        tabPanel("Lluvia acumulada", plotlyOutput("distPlot")),
        tabPanel("Porcentaje de datos perdidos", div(style = 'width:500px;margin:auto', DTOutput("tbl")))
      )
    )
  )
)


# función del servidor para crear gráfico
server <- function(input, output, session) {
  observe({
    if (input$checkbox) {
      updateSelectizeInput(session, 'est', choices = vars2)
    } else {
      updateSelectizeInput(session, 'est', choices = vars1)
    }
  })
  
  selectedData <- reactive({
    data_plot %>% dplyr::filter(Estacion==input$est) %>%
      dplyr::select(Estacion, est_id, fecha, qc_lluvia_acum, valores)
  })
    
    output$tbl = renderDT(perc_per, 
                                 options = list(autoWidth = TRUE,
                                                scrollY = 300)
      )
    
    output$distPlot <- renderPlotly({

      pp <- ggplot(selectedData(), aes(x=fecha, y=valores, color=qc_lluvia_acum))+geom_line()+theme_bw(16)+
                 theme(strip.background = element_blank())+labs(y="lluvia acumulada (mm)")
      
      plot_ly(selectedData(), x=~selectedData()$fecha, y = ~selectedData()$valores, color = ~selectedData()$qc_lluvia_acum, mode="lines+markers") %>%
        layout(title = list(text = paste0('Lluvia acumulada ', input$est), x=0),
               xaxis = list(title = 'fecha'), 
               yaxis = list(title = 'lluvia acumulada (mm)'),
               legend = list(orientation = 'h', xanchor = 'left', y=1.0, title = list(text = 'Lluvia acumulada')))
      
      #ggplotly(pp) #%>% layout(legend = list(orientation = 'h', xanchor = 'left', y=1.1, title = list(text = 'Lluvia corregida')))
      
    })
    
    output$download <- downloadHandler(
      filename = "data.csv",
      content = function(file) {
        readr::write_csv(selectedData(), file)
      }
    )

}

shinyApp(ui = ui, server = server, options=list(host = "0.0.0.0", port = 5050))