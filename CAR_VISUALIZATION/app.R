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
        # Filter data and calculate statistics
        avg_prices <- car_data %>%
            mutate(price = as.numeric(gsub("[$,]", "", price))) %>%
            filter(make == input$make_select,
                   year == input$year_select) %>%
            group_by(model) %>%
            summarise(
                avg_price = mean(price, na.rm = TRUE),
                count = n()
            ) %>%
            ungroup() %>%
            arrange(desc(avg_price))
        
        # Calculate the scaling factor for secondary axis
        price_range <- range(avg_prices$avg_price)
        count_range <- range(avg_prices$count)
        scale_factor <- diff(price_range)/diff(count_range)
        
        # Create the combined plot
        ggplot(avg_prices, aes(x = reorder(model, -avg_price))) +
            # Bar chart for prices
            geom_bar(aes(y = avg_price), 
                    stat = "identity", 
                    fill = "steelblue",
                    alpha = 0.7) +
            # Line chart for counts
            geom_line(aes(y = count * scale_factor, 
                         group = 1), 
                     color = "darkred",
                     size = 1.2) +
            geom_point(aes(y = count * scale_factor),
                      color = "darkred",
                      size = 3) +
            # Price labels on bars
            geom_text(aes(y = avg_price,
                         label = scales::dollar(avg_price, accuracy = 1)),
                     vjust = -0.5,
                     size = 3) +
            # Count labels on points
            geom_text(aes(y = count * scale_factor,
                         label = count),
                     vjust = -0.5,
                     color = "darkred",
                     size = 3) +
            # Primary axis (prices)
            scale_y_continuous(
                name = "Average Price ($)",
                labels = scales::dollar_format(),
                # Secondary axis (counts)
                sec.axis = sec_axis(
                    ~./scale_factor,
                    name = "Number of Vehicles",
                    labels = scales::number_format()
                )
            ) +
            theme_minimal() +
            labs(title = paste("Vehicle Analysis for", input$year_select, input$make_select),
                 x = "Model") +
            theme(
                axis.text.x = element_text(angle = 45, hjust = 1),
                plot.title = element_text(hjust = 0.5),
                axis.title.y.left = element_text(color = "steelblue"),
                axis.text.y.left = element_text(color = "steelblue"),
                axis.title.y.right = element_text(color = "darkred"),
                axis.text.y.right = element_text(color = "darkred")
            )
    })
}

# Run the application 
shinyApp(ui = ui, server = server)
