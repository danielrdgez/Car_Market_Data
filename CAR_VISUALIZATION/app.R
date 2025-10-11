#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    https://shiny.posit.co/
#

# Remove the install.packages() calls and replace with package checking
required_packages <- c("shiny", "tidyverse", "plotly")
new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

# Load required libraries
library(shiny)
library(tidyverse)
library(plotly)

# Get the latest CSV file from the output directory and print its name
latest_csv <- list.files(
  path = '/Users/OneTwo/Documents/CAR_ML/CAR_DATA_CLEAN/',
  pattern = "CLEAN_CAR_DATA_.*\\.csv$",
  full.names = TRUE
) |> sort() |> tail(1)

# Print the CSV filename to console
cat("Using CSV file:", basename(latest_csv), "\n")

# Add error handling for CSV reading
tryCatch({
  car_data <- read_csv(latest_csv)
}, error = function(e) {
  stop("Error reading CSV file: ", e$message)
})

# Define UI for application
ui <- fluidPage(

    # Application title
    titlePanel("Car Market Visualization : Florida"),

    # Sidebar with dropdowns for make and year selection
    sidebarLayout(
        sidebarPanel(
            selectInput("make_select",
                       "Select Make:",
                       choices = unique(car_data$make),
                       selected = NULL),
            selectInput("year_select",
                       "Select Year:",
                       choices = sort(unique(car_data$year), decreasing = TRUE),
                       selected = NULL)
        ),

        # Show a plot of the generated distribution
        mainPanel(
           plotlyOutput("distPlot")
        )
    )
)

# Define server logic
server <- function(input, output) {
    output$distPlot <- renderPlotly({
        # Data preparation
        avg_prices <- car_data %>%
            mutate(price = as.numeric(gsub("[$,]", "", price))) %>%
            filter(make == input$make_select,
                   year == input$year_select) %>%
            group_by(model) %>%
            summarise(
                avg_price = mean(price, na.rm = TRUE)
            ) %>%
            ungroup() %>%
            arrange(desc(avg_price))
        
        # Create plotly bar chart
        plot_ly(avg_prices, 
                x = ~reorder(model, -avg_price), 
                y = ~avg_price,
                type = 'bar',
                marker = list(color = 'steelblue'),
                text = ~scales::dollar(avg_price, accuracy = 1),
                textposition = 'outside') %>%
            layout(
                title = list(
                    text = paste("Average Car Prices for", input$year_select, input$make_select),
                    x = 0.5
                ),
                xaxis = list(
                    title = "Model",
                    tickangle = 45
                ),
                yaxis = list(
                    title = "Average Price ($)",
                    tickformat = "$,.0f"
                ),
                showlegend = FALSE,
                margin = list(b = 100)  # Add bottom margin for rotated labels
            )
    })
}

# Run the application 
shinyApp(ui = ui, server = server)
