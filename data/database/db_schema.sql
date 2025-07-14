-- phpMyAdmin SQL Dump
-- version 5.0.1
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: 2020-07-13 15:16:05
-- Server version: 8.0.21
-- PHP Version: 7.2.24

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `triplextract`
--

-- --------------------------------------------------------

--
-- Table structure for table `association`
--

CREATE TABLE `association` (
  `id` int UNSIGNED NOT NULL,
  `spec_id` int UNSIGNED NOT NULL,
  `gene_id` int UNSIGNED DEFAULT NULL,
  `trait_id` varchar(12) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `author`
--

CREATE TABLE `author` (
  `id` int UNSIGNED NOT NULL,
  `doc_id` int UNSIGNED NOT NULL,
  `first_name` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `last_name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `document`
--

CREATE TABLE `document` (
  `id` int UNSIGNED NOT NULL,
  `title` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `doi` varchar(300) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `pubmed_id` varchar(50) NOT NULL,
  `pmc_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `sici` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `publisher_id` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `year` int UNSIGNED NOT NULL,
  `journal` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `volume` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `gene_synonym`
--

CREATE TABLE `gene_synonym` (
  `id` int UNSIGNED NOT NULL,
  `tax_id` int DEFAULT NULL,
  `ncbi_id` int UNSIGNED DEFAULT NULL,
  `plaza_id` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `ncbi_synonyms` varchar(5000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '',
  `plaza_synonyms` varchar(5000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '',
  `symbol` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `locus_tag` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `db_xref` varchar(2500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `source` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `ncbi_gene2accession`
--

CREATE TABLE `ncbi_gene2accession` (
  `tax_id` int UNSIGNED NOT NULL,
  `gene_id` int UNSIGNED NOT NULL,
  `rna_nucl_acc_version` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `protein_acc_version` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `gen_nucl_acc_version` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `symbol` varchar(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `paragraph`
--

CREATE TABLE `paragraph` (
  `id` int UNSIGNED NOT NULL,
  `doc_id` int UNSIGNED NOT NULL,
  `section_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `text` varchar(20000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `plaza_gene_synonym`
--

CREATE TABLE `plaza_gene_synonym` (
  `id` int NOT NULL,
  `tax_id` int UNSIGNED NOT NULL,
  `plaza_id` varchar(100) NOT NULL,
  `synonym` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `plaza_orthology`
--

CREATE TABLE `plaza_orthology` (
  `id` int UNSIGNED NOT NULL,
  `query_tax_id` int UNSIGNED NOT NULL,
  `query_gene` varchar(200) NOT NULL,
  `ortho_tax_id` int UNSIGNED NOT NULL,
  `ortho_gene` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `ortho_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `plaza_species_id`
--

CREATE TABLE `plaza_species_id` (
  `tax_id` int UNSIGNED NOT NULL,
  `plaza_id` varchar(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `species_synonym`
--

CREATE TABLE `species_synonym` (
  `id` int UNSIGNED NOT NULL,
  `ncbi_synonyms` varchar(3000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `pubtator_synonyms` varchar(5000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `tm_association_type`
--

CREATE TABLE `tm_association_type` (
  `id` int UNSIGNED NOT NULL,
  `description` varchar(200) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `tm_association_type`
--

INSERT INTO `tm_association_type` (`id`, `description`) VALUES
(1, '1a'),
(2, '1b'),
(3, '1c'),
(4, '1d'),
(5, '2a'),
(6, '2ba'),
(7, '2bb'),
(8, '2c'),
(9, '2d');

-- --------------------------------------------------------

--
-- Table structure for table `tm_evidence`
--

CREATE TABLE `tm_evidence` (
  `id` int UNSIGNED NOT NULL,
  `assoc_id` int UNSIGNED NOT NULL,
  `doc_id` int UNSIGNED NOT NULL,
  `par_id` int UNSIGNED DEFAULT NULL,
  `spec_ann_id` int UNSIGNED DEFAULT NULL,
  `gene_ann_id` int UNSIGNED NOT NULL,
  `trait_ann_id` int UNSIGNED NOT NULL,
  `trait_synonym` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `type_id` int UNSIGNED NOT NULL,
  `score` int UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `tm_gene_annotation`
--

CREATE TABLE `tm_gene_annotation` (
  `id` int UNSIGNED NOT NULL,
  `par_id` int UNSIGNED NOT NULL,
  `gene_id` int UNSIGNED NOT NULL,
  `offset` int UNSIGNED NOT NULL,
  `length` int UNSIGNED NOT NULL,
  `text` varchar(200) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `tm_species_annotation`
--

CREATE TABLE `tm_species_annotation` (
  `id` int UNSIGNED NOT NULL,
  `par_id` int UNSIGNED NOT NULL,
  `spec_id` int UNSIGNED NOT NULL,
  `offset` int NOT NULL,
  `length` int UNSIGNED NOT NULL,
  `text` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `tm_trait_annotation`
--

CREATE TABLE `tm_trait_annotation` (
  `id` int UNSIGNED NOT NULL,
  `par_id` int UNSIGNED NOT NULL,
  `trait_id` varchar(12) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `offset` int NOT NULL,
  `length` int UNSIGNED NOT NULL,
  `text` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `trait_synonym`
--

CREATE TABLE `trait_synonym` (
  `id` varchar(12) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `synonyms` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `association`
--
ALTER TABLE `association`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `idx_association` (`spec_id`,`gene_id`,`trait_id`) USING BTREE,
  ADD KEY `fk__gene-id__ass` (`gene_id`),
  ADD KEY `fk__trait-id__ass` (`trait_id`);

--
-- Indexes for table `author`
--
ALTER TABLE `author`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx__doc-auth-id` (`doc_id`);

--
-- Indexes for table `document`
--
ALTER TABLE `document`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `gene_synonym`
--
ALTER TABLE `gene_synonym`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `idx__plaza_id` (`plaza_id`) USING BTREE,
  ADD UNIQUE KEY `idx__ncbi_id` (`ncbi_id`) USING BTREE;

--
-- Indexes for table `paragraph`
--
ALTER TABLE `paragraph`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx__doc-par-id` (`doc_id`);

--
-- Indexes for table `plaza_gene_synonym`
--
ALTER TABLE `plaza_gene_synonym`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx__plaza-syn` (`synonym`),
  ADD KEY `idx__plaza-tax-id` (`tax_id`),
  ADD KEY `idx__plaza-id` (`plaza_id`);

--
-- Indexes for table `plaza_orthology`
--
ALTER TABLE `plaza_orthology`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx__query-tax-id` (`query_tax_id`),
  ADD KEY `idx__orth-tax-id` (`ortho_tax_id`);

--
-- Indexes for table `plaza_species_id`
--
ALTER TABLE `plaza_species_id`
  ADD UNIQUE KEY `idx__ncbi-id` (`tax_id`);

--
-- Indexes for table `species_synonym`
--
ALTER TABLE `species_synonym`
  ADD UNIQUE KEY `idx_id` (`id`);

--
-- Indexes for table `tm_association_type`
--
ALTER TABLE `tm_association_type`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `tm_evidence`
--
ALTER TABLE `tm_evidence`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx__ass-ev__spec-ann-id` (`spec_ann_id`),
  ADD KEY `idx__ass-ev__gene-ann-id` (`gene_ann_id`),
  ADD KEY `idx__ass-ev__trait-ann-id` (`trait_ann_id`),
  ADD KEY `ids__ass-ev__ass-id` (`assoc_id`) USING BTREE,
  ADD KEY `idx__ass-ev__doc-id` (`doc_id`) USING BTREE,
  ADD KEY `idx__ass-ev__par-id` (`par_id`) USING BTREE,
  ADD KEY `idx__ass-ev__type` (`type_id`) USING BTREE;

--
-- Indexes for table `tm_gene_annotation`
--
ALTER TABLE `tm_gene_annotation`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx__gen-ann__par-id` (`par_id`) USING BTREE,
  ADD KEY `idx__gen-ann__gene-id` (`gene_id`) USING BTREE;

--
-- Indexes for table `tm_species_annotation`
--
ALTER TABLE `tm_species_annotation`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx__spec-ann__spec-id` (`spec_id`) USING BTREE,
  ADD KEY `idx__spec-ann__par-id` (`par_id`) USING BTREE;

--
-- Indexes for table `tm_trait_annotation`
--
ALTER TABLE `tm_trait_annotation`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx__trait-ann__par-id` (`par_id`),
  ADD KEY `idx__trait-ann__trait-id` (`trait_id`);

--
-- Indexes for table `trait_synonym`
--
ALTER TABLE `trait_synonym`
  ADD PRIMARY KEY (`id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `association`
--
ALTER TABLE `association`
  MODIFY `id` int UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `author`
--
ALTER TABLE `author`
  MODIFY `id` int UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `document`
--
ALTER TABLE `document`
  MODIFY `id` int UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `gene_synonym`
--
ALTER TABLE `gene_synonym`
  MODIFY `id` int UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `paragraph`
--
ALTER TABLE `paragraph`
  MODIFY `id` int UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `plaza_gene_synonym`
--
ALTER TABLE `plaza_gene_synonym`
  MODIFY `id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `plaza_orthology`
--
ALTER TABLE `plaza_orthology`
  MODIFY `id` int UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `tm_association_type`
--
ALTER TABLE `tm_association_type`
  MODIFY `id` int UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=10;

--
-- AUTO_INCREMENT for table `tm_evidence`
--
ALTER TABLE `tm_evidence`
  MODIFY `id` int UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `tm_gene_annotation`
--
ALTER TABLE `tm_gene_annotation`
  MODIFY `id` int UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `tm_species_annotation`
--
ALTER TABLE `tm_species_annotation`
  MODIFY `id` int UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `tm_trait_annotation`
--
ALTER TABLE `tm_trait_annotation`
  MODIFY `id` int UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `association`
--
ALTER TABLE `association`
  ADD CONSTRAINT `fk__gene-id__ass` FOREIGN KEY (`gene_id`) REFERENCES `gene_synonym` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `fk__species-id__ass` FOREIGN KEY (`spec_id`) REFERENCES `species_synonym` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `fk__trait-id__ass` FOREIGN KEY (`trait_id`) REFERENCES `trait_synonym` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Constraints for table `author`
--
ALTER TABLE `author`
  ADD CONSTRAINT `fk__doc-auth-id` FOREIGN KEY (`doc_id`) REFERENCES `document` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Constraints for table `paragraph`
--
ALTER TABLE `paragraph`
  ADD CONSTRAINT `fk__doc-par-id` FOREIGN KEY (`doc_id`) REFERENCES `document` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Constraints for table `plaza_gene_synonym`
--
ALTER TABLE `plaza_gene_synonym`
  ADD CONSTRAINT `fk__plaza-tax-id` FOREIGN KEY (`tax_id`) REFERENCES `species_synonym` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Constraints for table `plaza_orthology`
--
ALTER TABLE `plaza_orthology`
  ADD CONSTRAINT `fk__orth-tax-id` FOREIGN KEY (`ortho_tax_id`) REFERENCES `species_synonym` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `fk__query-tax-id` FOREIGN KEY (`query_tax_id`) REFERENCES `species_synonym` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Constraints for table `plaza_species_id`
--
ALTER TABLE `plaza_species_id`
  ADD CONSTRAINT `fk__ncbi-id` FOREIGN KEY (`tax_id`) REFERENCES `species_synonym` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Constraints for table `tm_evidence`
--
ALTER TABLE `tm_evidence`
  ADD CONSTRAINT `fk__ass-ev__ass-id` FOREIGN KEY (`assoc_id`) REFERENCES `association` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `fk__ass-ev__ass-type-id` FOREIGN KEY (`type_id`) REFERENCES `tm_association_type` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  ADD CONSTRAINT `fk__ass-ev__doc-id` FOREIGN KEY (`doc_id`) REFERENCES `document` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `fk__ass-ev__gene-ann-id` FOREIGN KEY (`gene_ann_id`) REFERENCES `tm_gene_annotation` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `fk__ass-ev__par-id` FOREIGN KEY (`par_id`) REFERENCES `paragraph` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `fk__ass-ev__spec-ann-id` FOREIGN KEY (`spec_ann_id`) REFERENCES `tm_species_annotation` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `fk__ass-ev__trait-ann-id` FOREIGN KEY (`trait_ann_id`) REFERENCES `tm_trait_annotation` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Constraints for table `tm_gene_annotation`
--
ALTER TABLE `tm_gene_annotation`
  ADD CONSTRAINT `fk__gen-ann__gene-id` FOREIGN KEY (`gene_id`) REFERENCES `gene_synonym` (`ncbi_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `fk__gen-ann__par-id` FOREIGN KEY (`par_id`) REFERENCES `paragraph` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Constraints for table `tm_species_annotation`
--
ALTER TABLE `tm_species_annotation`
  ADD CONSTRAINT `fk__spec-ann__par-id` FOREIGN KEY (`par_id`) REFERENCES `paragraph` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `fk__spec-ann__spec-id` FOREIGN KEY (`spec_id`) REFERENCES `species_synonym` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Constraints for table `tm_trait_annotation`
--
ALTER TABLE `tm_trait_annotation`
  ADD CONSTRAINT `fk__trait-ann__par-id` FOREIGN KEY (`par_id`) REFERENCES `paragraph` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `fk__trait-ann__trait-id` FOREIGN KEY (`trait_id`) REFERENCES `trait_synonym` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
