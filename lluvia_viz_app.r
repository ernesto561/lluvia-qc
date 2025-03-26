library(shiny)
library(plotly)
library(dplyr)
library(readr)
library(DT)

# Define UI for application
ui <-fluidPage(

  # Application title
  titlePanel("Control de calidad de lluvia"),

  # Sidebar with a slider input
  sidebarLayout(
    sidebarPanel(
      checkboxInput("checkbox", "Mostrar solo estaciones con errores", FALSE),
      selectizeInput("est", "Estación", choices= vars1, selected = NULL, multiple = FALSE,
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
  print(paste("Server working directory:", getwd()))
  observe({
    if (input$checkbox) {
      updateSelectizeInput(session, 'est', choices = vars2)
    } else {
      updateSelectizeInput(session, 'est', choices = vars1)
    }
  })

  selectedData <- reactive({
    data_plot %>% dplyr::filter(estacion==input$est) %>%
      dplyr::select(estacion, est_id, fecha, qc_lluvia_acum, valores)
  })

    output$tbl = renderDT(perc_per,
                                 options = list(autoWidth = TRUE,
                                                scrollY = 300)
      )

    output$distPlot <- renderPlotly({
      plot_ly(selectedData(), x=~selectedData()$fecha, y = ~selectedData()$valores, color = ~selectedData()$qc_lluvia_acum, mode="lines+markers") %>%
        layout(title = list(text = paste0('Lluvia acumulada ', input$est), x=0),
               xaxis = list(title = 'fecha'),
               yaxis = list(title = 'lluvia acumulada (mm)'),
               legend = list(orientation = 'h', 
                           xanchor = 'center', 
                           x = 0.5,
                           y = 1.1))
    })

    output$download <- downloadHandler(
      filename = "data.csv",
      content = function(file) {
        readr::write_csv(selectedData(), file)
      }
    )

}

shinyApp(ui = ui, server = server, options=list(host = "0.0.0.0", port = 5050))