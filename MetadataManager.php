<?php

class MetadataManager {

    private $metadataContainerName;
    private $metadataDefaultResourceName;
    private $cache;
    private $metadataStorageRequest;
    private $storageManager;


    public function __construct(array $params, StorageManager $storageManager)
    {
        // Parameters
        if (!array_key_exists('metadata_container_name', $params)) {
            throw new Exception('metadata_container_name missing');
        }
        if (!array_key_exists('metadata_default_resource_name', $params)) {
            throw new Exception('metadata_default_resource_name missing');
        }
        $this->metadataContainerName = $params['metadata_container_name'];
        $this->metadataDefaultResourceName = $params['metadata_default_resource_name'];

        // Setup cache
        $this->cache = array();

        // Storage manager for fetching entity metadata
        $this->storageManager = $storageManager;
    }


    public function metadataForEntity($entityName)
    {
        // Try cache
        $cacheName = 'Entity.' . $entityName;
        if (!array_key_exists($cacheName, $this->cache)) {
            // Cache miss. Fetch from store
            $request = $this->getMetadataStorageRequest();
            $request->setFilter(array('description.shortName' => $entityName));
            $metadata = $this->storageManager->fetchOne($request);
            if (!$metadata) {
                throw new Exception('Could not locate metadata for entity: [' . $entityName . ']');
            }

            // Populate new EntityMetadata
            $entityMetadata = new EntityMetadata();
            $entityMetadata->setData($metadata);

            $this->cache[$cacheName] = $entityMetadata;
        }

        return $this->cache[$cacheName];
    }


    public function metadataForDefaultResourceName($defaultResourceName)
    {
        // Get this and above to share code in a private method

        // filter on name

        throw new Exception('Implement me');
    }


    public function getNavigationProperty($entityName, $navigationPropertyName)
    {
        // This could be simple, or complex depending on the type of relationship.
        // It could involve scanning all entities looking for a matching associationName to get foreign keys and inverse foreign keys
        // So far we only have defined 1:1 and 1:many without inverse relationships, which defines a single key to a primary key.
        // We will enforce this pattern until other methods can be verified.

        $entityKey = null;
        $relatedEntityKey = null;

        // Get the entity metadata
        $entityMetadata = $this->metadataForEntity($entityName);

        // Get the raw property data
        $entityRaw = $entityMetadata->getNavigationPropertyFields($navigationPropertyName);
        if (!$entityRaw) {
            throw new Exception(sprintf('Navigation property: [%s] does not exist on entity: [%s]', $navigationPropertyName, $entityName));
        }

        // First big question - is it scalar?
        if ($entityRaw['isScalar']) {
            // Yes, it's a 1:1 relationship. We should have a foreignKey defined which maps to the primary key of the host entity.
            if (!array_key_exists('foreignKeyNames', $entityRaw) || !count($entityRaw['foreignKeyNames'])) {
                throw new Exception(sprintf('Illegal navigation property definition [%s.%s]. A 1:1 property must have foreignKeyNames defined', $entityName, $navigationPropertyName));
            }

            // Not handling compound keys yet!
            $entityKey = $entityRaw['foreignKeyNames'][0];

            // Now get the related entity metadata, so we can get it's primary key
            $stripNamespace = true;
            $relatedEntityName = NavigationProperty::getEntityTypeName($entityRaw, $stripNamespace);
            $relatedEntityMetadata = $this->metadataForEntity($relatedEntityName);
            $relatedEntityKey = $relatedEntityMetadata->getKey();
            if (!$relatedEntityKey) {
                throw new Exception(sprintf('Illegal navigation property definition [%s.%s]. A 1:1 navigation property needs a primary key defined on the related entity', $entityName, $navigationPropertyName));
            }
        } else {
            // No, it's 1:many relationship. There are probably multiple ways to define this but at the moment we only accept
            // an invForeignKeys mapped to the primary key of the host entity.
            if (!array_key_exists('invForeignKeyNames', $entityRaw) || !count($entityRaw['invForeignKeyNames'])) {
                throw new Exception(sprintf('Illegal navigation property definition [%s.%s]. A 1:1 property must have foreignKeyNames defined', $entityName, $navigationPropertyName));
            }

            // Not handling compound keys yet!
            $relatedEntityKey = $entityRaw['invForeignKeyNames'][0];

            // Get the primary key of this entity (not handling compound primary keys...)
            $entityKey = $entityMetadata->getKey();
            if (!$entityKey) {
                throw new Exception(sprintf('Illegal navigation property definition [%s.%s]. A navigation property needs a primary key defined', $entityName, $navigationPropertyName));
            }
        }

        return new NavigationProperty($entityKey, $relatedEntityKey);
    }


    private function getMetadataStorageRequest()
    {
        if (!$this->metadataStorageRequest) {
            // Create request for metadata - we have to provide metadata so the request knows the correct container to use.
            // How can the metadata get metadata without metadata?
            // We have to mock the parts of the metadata the storage manager needs
            $metadata = new EntityMetadata();
            $metadata->setContainerName($this->metadataContainerName);

            // Create storage request, which we can reuse
            $this->metadataStorageRequest = new StorageRequest($this->metadataDefaultResourceName, $metadata);
        }
        return $this->metadataStorageRequest;
    }
} 