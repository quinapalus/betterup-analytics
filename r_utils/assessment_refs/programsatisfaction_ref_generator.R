# Create Reference .csv for psycap assessment
library(dplyr)
library(stringr)
library(purrr)
library(readr)
library(yaml)

# Load Data
yml_raw <- list.files("assessment_refs/raw_data", "program_satisfaction*", full.names = TRUE)
yml_read <- map(yml_raw, yaml.load_file)
#onboard_ref <- read_csv("assessment_refs/raw_data/onboarding_reference.csv")

scale_maker <- function(xlist) {
  if (is_null(xlist[[1]])) {
    return("text_response")
  }

  paste_fun <- function(scale) {
    if (scale[[1]] == scale[[2]]) {
      return(scale[[1]])
    }

    paste(scale[[1]], scale[[2]], sep = "-")
  }

  xlist %>% map_chr(paste_fun) %>% str_c(collapse = ";")
}

# Parse into data frame
make_df <- function(cm_yml) {
  data_frame(
    item_key = names(cm_yml$questions),
    item_prompt = map_chr(cm_yml$questions, "label"),
    scale = map(cm_yml$questions, "options") %>% map_chr(scale_maker)
  )
}

# Export
df_exp <- map_dfr(yml_read, make_df) %>% distinct() %>% arrange(item_key)
write_csv(df_exp, "../warehouse/data/item_definition_program_satisfaction.csv")
