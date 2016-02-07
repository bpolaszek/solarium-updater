<?php
namespace BenTools\SolariumUpdater;
use Solarium\Core\Client\Client as Solarium;
use Solarium\Plugin\BufferedAdd\BufferedAdd;
use Solarium\Plugin\PrefetchIterator;
use Solarium\QueryType\Select\Query\Query;
use Solarium\QueryType\Select\Result\Document as SelectDocument;
use Solarium\QueryType\Update\Query\Document\Document as UpdateDocument;

class SolariumUpdater {

    /**
     * @var Solarium
     */
    protected $solarium;

    protected $buffer = 200;

    /**
     * SolrUpdater constructor.
     * @param Solarium $solarium
     * @param int $buffer
     */
    public function __construct(Solarium $solarium, $buffer = 200) {
        $this->solarium = $solarium;
        $this->buffer   = $buffer;
    }

    /**
     * @param Query $query
     * @param callable $transform
     * @param null $readEndpoint
     * @param null $writeEndpoint
     * @param bool $commit
     */
    public function updateBufferedPlugin(Query $query, callable $transform = null, $readEndpoint = null, $writeEndpoint = null, $commit = true) {

        /**
         * @var PrefetchIterator $prefetch
         * @var BufferedAdd $update
         * @var SelectDocument $document
         */

        $prefetch = $this->getSolarium()->getPlugin('prefetchiterator');
        $prefetch->setPrefetch($this->getBuffer());
        $prefetch->setQuery($query);
        $prefetch->setEndpoint($readEndpoint);

        $update = $this->solarium->getPlugin('bufferedadd');
        $update->setBufferSize($this->getBuffer());
        $update->setEndpoint($writeEndpoint);

        foreach ($prefetch AS $document) {
            if (is_callable($transform))
                $document = $transform($document);
            else
                $document = new UpdateDocument($document->getFields());
            $update->addDocument($document);
        }

        $update->flush();

        if ($commit === true)
            $update->commit();
    }

    /**
     * @param Query $query
     * @param callable $transform
     * @param null $readEndpoint
     * @param null $writeEndpoint
     * @param bool $commit
     * @internal param null $endpoint
     */
    public function updateBufferedNoPlugin(Query $query, callable $transform, $readEndpoint = null, $writeEndpoint = null, $commit = true) {

        $nbRows = count($this->solarium->execute($query->setRows(0), $readEndpoint));
        $update = $this->solarium->createUpdate();

        for ($offset = 0; $offset <= $nbRows; $offset += $this->buffer) {

            $query->setStart($offset)->setRows($this->buffer);
            $documents = $this->solarium->execute($query, $readEndpoint);
            foreach ($documents AS $document)
                $update->addDocument($transform($document));

            $this->solarium->execute($update, $writeEndpoint);
            $update = $this->solarium->createUpdate();
        }

        if ($nbRows > 0 && $commit === true)
            $this->solarium->execute($update->addCommit(), $writeEndpoint);
    }

    /**
     * @param Query $query
     * @param callable $transform
     * @param null $readEndpoint
     * @param null $writeEndpoint
     */
    public function updateNoBuffer(Query $query, callable $transform, $readEndpoint = null, $writeEndpoint = null, $commit = true) {

        $update = $this->solarium->createUpdate();

        $documents = $this->solarium->execute($query, $readEndpoint);
        foreach ($documents AS $document)
            $update->addDocument($transform($document));

        if (count($documents) > 0 && $commit === true)
            $update->addCommit();

        $this->solarium->execute($update, $writeEndpoint);
    }

    /**
     * @return Solarium
     */
    public function getSolarium() {
        return $this->solarium;
    }

    /**
     * @param Solarium $solarium
     * @return $this - Provides Fluent Interface
     */
    public function setSolarium($solarium) {
        $this->solarium = $solarium;
        return $this;
    }

    /**
     * @return int
     */
    public function getBuffer() {
        return $this->buffer;
    }

    /**
     * @param int $buffer
     * @return $this - Provides Fluent Interface
     */
    public function setBuffer($buffer) {
        $this->buffer = $buffer;
        return $this;
    }
}