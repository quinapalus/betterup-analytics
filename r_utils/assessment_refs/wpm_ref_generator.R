# Create reference .csv for WPM

library(dplyr)
library(stringr)
library(purrr)
library(readr)
library(yaml)

# Load Data
wp_ref <- yaml.load_file("assessment_refs/raw_data/whole_person.yml")
subd_ref <- yaml.load_file("assessment_refs/raw_data/whole_person_subdimensions.yml")
onboard_ref <- read_csv("assessment_refs/raw_data/onboarding_reference.csv")

# Extract Scale Information


# Create Dataframe
scale_maker <- function(xlist) {
    xlist %>% map_chr(~paste(.x[[1]], .x[[2]], sep = "-")) %>% str_c(collapse = ";")
}

wpm_df <-
  data_frame(
    item_key = names(wp_ref$questions),
    subdimension = map_chr(wp_ref$questions, "subdimension"),
    item_prompt = map_chr(wp_ref$questions, "label"),
    scale = map(wp_ref$questions, "options") %>% map_chr(scale_maker)
  )

subd_df <-
  data_frame(
    subd_code = names(subd_ref),
    subd_display_label = map_chr(subd_ref, "label")
  )

# Join Data
wpm_exp <-
  inner_join(wpm_df, onboard_ref, by = c("item_key" = "question_code")) %>%
  select(-label, -sub_dimension, -survey)

# Write to directory
write_csv(wpm_exp, "../warehouse/data/item_definition_whole_person.csv")
