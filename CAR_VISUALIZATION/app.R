#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    https://shiny.posit.co/
#

# Remove the install.packages() calls and replace with package checking
required_packages <- c("shiny", "tidyverse")
new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

# Load required libraries
library(shiny)
library(tidyverse)

# Get the latest CSV file from the output directory and print its name
latest_csv <- list.files(
  path = '/Users/OneTwo/Documents/CAR_ML/CAR_DATA_OUTPUT/',
  pattern = "CAR_DATA_.*\\.csv$",
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
    titlePanel("Average Car Price by Make and Model"),

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
           plotOutput("distPlot")
        )
    )
)

# Define server logic required to draw a bar chart
server <- function(input, output) {
    output$distPlot <- renderPlot({
        # Filter data based on selected make and year, then calculate average price by model
        avg_prices <- car_data %>%
            # Remove $ and , from price column and convert to numeric
            mutate(price = as.numeric(gsub("[$,]", "", price))) %>%
            filter(make == input$make_select,
                   year == input$year_select) %>%
            group_by(model) %>%
            summarise(avg_price = mean(price, na.rm = TRUE)) %>%
            ungroup() %>%
            # Arrange by average price
            arrange(desc(avg_price))
        
        # Create the bar chart
        ggplot(avg_prices, aes(x = reorder(model, avg_price), 
                              y = avg_price)) +
            geom_bar(stat = "identity", fill = "steelblue") +
            coord_flip() + # Flip coordinates for better label readability
            theme_minimal() +
            labs(title = paste("Average Car Price for", input$year_select, input$make_select, "Models"),
                 x = "Model",
                 y = "Average Price ($)") +
            theme(
                axis.text.y = element_text(size = 10),
                plot.title = element_text(hjust = 0.5)
            ) +
            scale_y_continuous(labels = scales::dollar_format())
    })
}

# Run the application 
shinyApp(ui = ui, server = server)
