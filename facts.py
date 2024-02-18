# The Ultimate List of Medical Specialties and Subspecialties
# https://www.sgu.edu/blog/medical/ultimate-list-of-medical-specialties/
def is_valid_specialty(specialty):
    doctor_specialties = [
    'Allergy and Immunology',
    'Anesthesiology',
        'Critical care medicine',
        'Hospice and palliative care',
        'Pain medicine',
        'Pediatric anesthesiology',
        'Sleep medicine',
    'Dermatology',
        'Dermatopathology',
        'Pediatric dermatology',
        'Procedural dermatology',
    'Diagnostic radiology',
        'Abdominal radiology',
        'Breast imaging',
        'Cardiothoracic radiology',
        'Cardiovascular radiology',
        'Chest radiology',
        'Emergency radiology',
        'Endovascular surgical neuroradiology',
        'Gastrointestinal radiology',
        'Genitourinary radiology',
        'Head and neck radiology',
        'Interventional radiology',
        'Musculoskeletal radiology',
        'Neuroradiology',
        'Nuclear radiology',
        'Pediatric radiology',
        'Radiation oncology',
        'Vascular and interventional radiology',
    'Emergency medicine',
        'Anesthesiology critical care medicine',
        'Emergency medical services',
        'Hospice and palliative medicine',
        'Internal medicine / Critical care medicine',
        'Medical toxicology',
        'Pain medicine',
        'Pediatric emergency medicine',
        'Sports medicine',
        'Undersea and hyperbaric medicine',
    'Family medicine',
        'Adolescent medicine',
        'Geriatric medicine',
        'Hospice and palliative medicine',
        'Pain medicine',
        'Sleep medicine',
        'Sports medicine',
    'Internal medicine',
        'Advanced heart failure and transplant cardiology',
        'Cardiovascular disease',
        'Clinical cardiac electrophysiology',
        'Critical care medicine',
        'Endocrinology, diabetes, and metabolism',
        'Gastroenterology',
        'Geriatric medicine',
        'Hematology',
        'Hematology and oncology',
        'Infectious disease',
        'Internal medicine',
        'Interventional cardiology',
        'Nephrology',
        'Oncology',
        'Pediatric internal medicine',
        'Pulmonary disease',
        'Pulmonary disease and critical care medicine',
        'Rheumatology',
        'Sleep medicine',
        'Sports medicine',
        'Transplant hepatology',
    'Medical genetics',
        'Biochemical genetics',
        'Clinical cytogenetics',
        'Clinical genetics',
        'Molecular genetic pathology',
    'Neurology',
        'Brain injury medicine',
        'Child neurology',
        'Clinical neurophysiology',
        'Endovascular surgical neuroradiology',
        'Hospice and palliative medicine',
        'Neurodevelopmental disabilities',
        'Neuromuscular medicine',
        'Pain medicine',
        'Sleep medicine',
        'Vascular neurology',
    'Nuclear medicine',
    'Obstetrics and gynecology',
        'Female pelvic medicine and reconstructive surgery',
        'Gynecologic oncology',
        'Maternal-fetal medicine',
        'Reproductive endocrinologists and infertility',
    'Ophthalmology',
        'Anterior segment/cornea ophthalmology',
        'Glaucoma ophthalmology',
        'Neuro-ophthalmology',
        'Ocular oncology',
        'Oculoplastics/orbit',
        'Ophthalmic Plastic & Reconstructive Surgery',
        'Retina/uveitis',
        'Strabismus/pediatric ophthalmology',
    'Pathology',
        'Anatomical pathology',
        'Blood banking and transfusion medicine',
        'Chemical pathology',
        'Clinical pathology',
        'Cytopathology',
        'Forensic pathology',
        'Genetic pathology',
        'Hematology',
        'Immunopathology',
        'Medical microbiology',
        'Molecular pathology',
        'Neuropathology',
        'Pediatric pathology',
    'Pediatrics',
        'Adolescent medicine',
        'Child abuse pediatrics',
        'Developmental-behavioral pediatrics',
        'Neonatal-perinatal medicine',
        'Pediatric cardiology',
        'Pediatric critical care medicine',
        'Pediatric endocrinology',
        'Pediatric gastroenterology',
        'Pediatric hematology-oncology',
        'Pediatric infectious diseases',
        'Pediatric nephrology',
        'Pediatric pulmonology',
        'Pediatric rheumatology',
        'Pediatric sports medicine',
        'Pediatric transplant hepatology',
    'Physical medicine and Rehabilitation',
        'Brain injury medicine',
        'Hospice and palliative medicine',
        'Neuromuscular medicine',
        'Pain medicine',
        'Pediatric rehabilitation medicine',
        'Spinal cord injury medicine',
        'Sports medicine',
    'Preventive medicine',
        'Aerospace medicine',
        'Medical toxicology',
        'Occupational medicine',
        'Public health medicine',
    'Psychiatry',
        'Addiction psychiatry',
        'Administrative psychiatry',
        'Child and adolescent psychiatry',
        'Community psychiatry',
        'Consultation/liaison psychiatry',
        'Emergency psychiatry',
        'Forensic psychiatry',
        'Geriatric psychiatry',
        'Mental retardation psychiatry',
        'Military psychiatry',
        'Pain medicine',
        'Psychiatric research',
        'Psychosomatic medicine',
    'Radiation oncology',
        'Hospice and palliative medicine',
        'Pain medicine',
    'Surgery',
        'Colon and rectal surgery',
        'General surgery',
        'Surgical critical care',
        'Gynecologic oncology',
        'Plastic surgery',
        'Craniofacial surgery',
        'Hand surgery',
        'Neurological surgery',
        'Endovascular surgical neuroradiology',
        'Ophthalmic surgery',
        'Oral and maxillofacial surgery',
        'Orthopaedic surgery',
        'Adult reconstructive orthopaedics',
        'Foot and ankle orthopaedics',
        'Musculoskeletal oncology',
        'Orthopaedic sports medicine',
        'Orthopaedic surgery of the spine',
        'Orthopaedic trauma',
        'Pediatric orthopaedics',
        'Otolaryngology',
        'Pediatric otolaryngology',
        'Otology neurotology',
        'Pediatric surgery',
        'Neonatal',
        'Prenatal',
        'Trauma',
        'Pediatric oncology',
        'Surgical Intensivists, specializing in critical care patients',
        'Thoracic Surgery',
        'Congenital cardiac surgery',
        'Thoracic surgery-integrated',
        'Vascular surgery',
    'Urology',
        'Pediatric urology',
        'Urologic oncology',
        'Renal transplant',
        'Male infertility',
        'Calculi',
        'Female urology',
        'Neurourology',
    ]
    doctor_specialties = [specialty.upper() for specialty in doctor_specialties]
    return specialty in doctor_specialties