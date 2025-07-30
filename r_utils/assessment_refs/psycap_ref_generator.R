# Create Reference .csv for psycap assessment
library(dplyr)
library(stringr)
library(purrr)
library(readr)
library(yaml)

# Load Data
psy_yml <- yaml.load_file("assessment_refs/raw_data/psycap_short.yml")
onboard_ref <- read_csv("assessment_refs/raw_data/onboarding_reference.csv")

scale_maker <- function(xlist) {
  xlist %>% map_chr(~paste(.x[[1]], .x[[2]], sep = "-")) %>% str_c(collapse = ";")
}

# Parse into data frame
psycap_df <-
  data_frame(
    item_key = names(psy_yml$questions),
    item_prompt = map_chr(psy_yml$questions, "label"),
    scale = map(psy_yml$questions, "options") %>% map_chr(scale_maker)
  )

# Export
psy_exp <-
  inner_join(psycap_df, onboard_ref, by = c("item_key" = "question_code")) %>%
  select(item_key, subdimension = sub_dimension, item_prompt, scale)

write_csv(psy_exp, "../warehouse/data/item_definition_psycap.csv")
