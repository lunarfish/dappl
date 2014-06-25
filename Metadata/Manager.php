<?php

namespace Dappl\Metadata;

use \Dappl\Storage\Manager as StorageManager;
use \Dappl\Fetch\Request;


class Manager {

    private $metadataContainerName;
    private $metadataDefaultResourceName;
    private $cache;
    private $metadataStorageRequest;
    private $storageManager;

    private $containerNames;


    const CACHE_PREFIX_ENTITY = 'Entity.';
    const CACHE_PREFIX_RESOURCE = 'Resource.';


    public function __construct(array $params, StorageManager $storageManager)
    {
        // Parameters
        if (!array_key_exists('metadata_container_name', $params)) {
            throw new \Exception('metadata_container_name missing');
        }
        if (!array_key_exists('metadata_default_resource_name', $params)) {
            throw new \Exception('metadata_default_resource_name missing');
        }
        $this->metadataContainerName = $params['metadata_container_name'];
        $this->metadataDefaultResourceName = $params['metadata_default_resource_name'];


        // temp measure
        $this->containerNames = isset($params['container_names']) ? $params['container_names'] : null;


        // Setup cache
        $this->cache = array();

        // Storage manager for fetching entity metadata
        $this->storageManager = $storageManager;
    }


    public function metadataForEntity($entityName)
    {
        // Try cache
        $cacheName = self::CACHE_PREFIX_ENTITY . $entityName;
        if (!array_key_exists($cacheName, $this->cache)) {
            // Cache miss. Fetch from store
            $request = $this->getMetadataStorageRequest();
            $request->setFilter(array('description.shortName' => $entityName));
            $metadata = $this->storageManager->fetchOne($request);
            if (!$metadata) {
                throw new \Exception('Could not locate metadata for entity: [' . $entityName . ']');
            }

            // Populate new EntityMetadata
            $entityMetadata = $this->createMetadataEntity($metadata);
            $this->addToCache($entityMetadata);
        }

        return $this->cache[$cacheName];
    }


    public function metadataForDefaultResourceName($defaultResourceName)
    {
        // Try cache
        $cacheName = self::CACHE_PREFIX_RESOURCE . $defaultResourceName;
        if (!array_key_exists($cacheName, $this->cache)) {
            // Cache miss. Fetch from store
            $request = $this->getMetadataStorageRequest();
            $request->setFilter(array('description.defaultResourceName' => $defaultResourceName));
            $metadata = $this->storageManager->fetchOne($request);
            if (!$metadata) {
                throw new \Exception('Could not locate metadata with default resource name: [' . $defaultResourceName . ']');
            }

            // Populate new EntityMetadata
            $entityMetadata = $this->createMetadataEntity($metadata);
            $this->addToCache($entityMetadata);
        }

        return $this->cache[$cacheName];
    }


    private function createMetadataEntity(array $metadata)
    {
        $entityMetadata = new Entity();
        $entityMetadata->setData($metadata);

        // Locate default resource name from raw data
        $defaultResourceName = isset($metadata['description']['defaultResourceName']) ? $metadata['description']['defaultResourceName'] : null;

        if ($this->containerNames &&
            $defaultResourceName &&
            array_key_exists($defaultResourceName, $this->containerNames)) {
            $entityMetadata->setContainerName($this->containerNames[$defaultResourceName]);
        }
        return $entityMetadata;
    }


    private function addToCache(Entity $metadata)
    {
        // Add to both entity and resource cache keys
        $this->cache[self::CACHE_PREFIX_ENTITY . $metadata->getEntityName()] = $metadata;
        $this->cache[self::CACHE_PREFIX_RESOURCE . $metadata->getDefaultResourceName()] = $metadata;
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
            throw new \Exception(sprintf('Navigation property: [%s] does not exist on entity: [%s]', $navigationPropertyName, $entityName));
        }

        $navigationProperty = new NavigationProperty($navigationPropertyName, $entityRaw);

        // First big question - is it scalar?
        if ($navigationProperty->isScalar()) {
            // Yes, it's a 1:1 relationship. We should have a foreignKey defined which maps to the primary key of the host entity.
            if (!array_key_exists('foreignKeyNames', $entityRaw) || !count($entityRaw['foreignKeyNames'])) {
                throw new \Exception(sprintf('Illegal navigation property definition [%s.%s]. A 1:1 property must have foreignKeyNames defined', $entityName, $navigationPropertyName));
            }

            // Not handling compound keys yet!
            $entityKey = $entityRaw['foreignKeyNames'][0];

            // Now get the related entity metadata, so we can get it's primary key
            $stripNamespace = true;
            $relatedEntityName = $navigationProperty->getEntityTypeName($stripNamespace);
            $relatedEntityMetadata = $this->metadataForEntity($relatedEntityName);
            $relatedEntityKey = $relatedEntityMetadata->getKey();
            if (!$relatedEntityKey) {
                throw new \Exception(sprintf('Illegal navigation property definition [%s.%s]. A 1:1 navigation property needs a primary key defined on the related entity', $entityName, $navigationPropertyName));
            }
        } else {
            // No, it's 1:many relationship. There are probably multiple ways to define this but at the moment we only accept
            // an invForeignKeys mapped to the primary key of the host entity.
            if (!array_key_exists('invForeignKeyNames', $entityRaw) || !count($entityRaw['invForeignKeyNames'])) {
                throw new \Exception(sprintf('Illegal navigation property definition [%s.%s]. A 1:1 property must have foreignKeyNames defined', $entityName, $navigationPropertyName));
            }

            // Not handling compound keys yet!
            $relatedEntityKey = $entityRaw['invForeignKeyNames'][0];

            // Get the primary key of this entity (not handling compound primary keys...)
            $entityKey = $entityMetadata->getKey();
            if (!$entityKey) {
                throw new \Exception(sprintf('Illegal navigation property definition [%s.%s]. A navigation property needs a primary key defined', $entityName, $navigationPropertyName));
            }
        }

        $navigationProperty->setKeys($entityKey, $relatedEntityKey);
        return $navigationProperty;
    }


    private function getMetadataStorageRequest()
    {
        if (!$this->metadataStorageRequest) {
            // Create request for metadata - we have to provide metadata so the request knows the correct container to use.
            // How can the metadata get metadata without metadata?
            // We have to mock the parts of the metadata the storage manager needs
            $metadata = new Entity();
            $metadata->setContainerName($this->metadataContainerName);

            // Create storage request, which we can reuse
            $this->metadataStorageRequest = new Request($this->metadataDefaultResourceName, $metadata);
        }
        return $this->metadataStorageRequest;
    }
} 