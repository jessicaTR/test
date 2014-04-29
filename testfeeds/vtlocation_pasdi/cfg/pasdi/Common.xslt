<?xml version = "1.0"?>
<xsl:stylesheet version="1.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

	<xsl:template name="el_simple">
		<xsl:param name="el" />
		<xsl:param name="elName"/>
		<xsl:param name="attr_languageId"/>
		<xsl:param name="attr_effectiveFrom"/>
		<xsl:param name="attr_effectiveTo"/>
		<xsl:param name="attr_objectTypeId"/>
		<xsl:param name="attr_objectType"/>
				
		<xsl:if test="string($el) != ''">
			<xsl:element name="{$elName}">
				<xsl:if test="string($attr_languageId) != ''">				
					<xsl:attribute name="languageId">
						<xsl:value-of select="string($attr_languageId)" />
					</xsl:attribute>
				</xsl:if>
				<xsl:if test="string($attr_effectiveFrom) != ''">
					<xsl:attribute name="effectiveFrom">
						<xsl:value-of select="string($attr_effectiveFrom)" />
					</xsl:attribute>
				</xsl:if>
				<xsl:if test="string($attr_effectiveTo) != ''">				
					<xsl:attribute name="effectiveTo">
						<xsl:value-of select="string($attr_effectiveTo)" />
					</xsl:attribute>
				</xsl:if>
				<xsl:if test="string($attr_objectTypeId) != ''">				
					<xsl:attribute name="objectTypeId">
						<xsl:value-of select="string($attr_objectTypeId)" />
					</xsl:attribute>
				</xsl:if>
				<xsl:if test="string($attr_objectType) != ''">				
					<xsl:attribute name="objectType">
						<xsl:value-of select="string($attr_objectType)" />
					</xsl:attribute>
				</xsl:if>
				<xsl:value-of select="string($el)" />
			</xsl:element>
		</xsl:if>
	</xsl:template>
	
	<xsl:template name="el_add_attr">
		<xsl:param name="base" />
		<xsl:param name="val" />
		<xsl:for-each select="$base/effective_from">
			<xsl:attribute name="effectiveFrom">
				<xsl:value-of select="string(.)" />
			</xsl:attribute>
		</xsl:for-each>
		<xsl:value-of select="string($val)" />
	</xsl:template>
	
	<xsl:template name="ves_gun_id">
		<xsl:param name="base" select="/.." />
		<xsl:param name="parentEl" select="/.." />
		<!-- done -->
		<xsl:if test="string($parentEl/relation_object_type_id) != ''">
			<xsl:attribute name="relationObjectTypeId">
				<xsl:value-of select="string($parentEl/relation_object_type_id)" />
			</xsl:attribute>
		</xsl:if>
		<!-- done -->
		<xsl:if test="string($parentEl/relation_object_type) != ''">
			<xsl:attribute name="relationObjectType">
				<xsl:value-of select="string($parentEl/relation_object_type)" />
			</xsl:attribute>
		</xsl:if>		
		<xsl:if test="string($parentEl/relationship_id) != ''">
			<xsl:attribute name="relationshipId">
				<xsl:value-of select="string($parentEl/relationship_id)" />
			</xsl:attribute>
		</xsl:if>
		<!-- done -->
		<xsl:if test="string($parentEl/relationship_type_id) != ''">
			<xsl:attribute name="relationshipTypeId">
				<xsl:value-of select="string($parentEl/relationship_type_id)" />
			</xsl:attribute>
		</xsl:if>		
		<!-- done -->
		<xsl:if test="string($base/cne_asset_id) != ''">
			<xsl:attribute name="relatedObjectId">
				<xsl:value-of select="string($base/cne_asset_id)" />
			</xsl:attribute>
		</xsl:if>
		<!-- done -->
		<xsl:if test="string($base/cne_asset_id/@object_type_id) != ''">
			<xsl:attribute name="relatedObjectTypeId">
				<xsl:value-of select="string($base/cne_asset_id/@object_type_id)" />
			</xsl:attribute>
		</xsl:if>
		<!-- done -->
		<xsl:if test="string($base/cne_asset_id/@object_type) != ''">
			<xsl:attribute name="relatedObjectType">
				<xsl:value-of select="string($base/cne_asset_id/@object_type)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($parentEl/relation_role) != ''">
			<xsl:attribute name="relationRole">
				<xsl:value-of select="string($parentEl/relation_role)" />
			</xsl:attribute>
		</xsl:if>
		<!-- done -->
		<xsl:if test="string($parentEl/relationship_type) != ''">
			<xsl:attribute name="relationshipType">
				<xsl:value-of select="string($parentEl/relationship_type)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($parentEl/relationship_confidence) != ''">
			<xsl:attribute name="relationshipConfidence">
				<xsl:value-of select="string($parentEl/relationship_confidence)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($parentEl/relation_object_na_code) != ''">
			<xsl:attribute name="relationObjectNACode">
				<xsl:value-of select="string($parentEl/relation_object_na_code)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($parentEl/related_object_order) != ''">
			<xsl:attribute name="relatedObjectOrder">
				<xsl:value-of select="string($parentEl/related_object_order)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($parentEl/relation_object_order) != ''">
			<xsl:attribute name="relationObjectOrder">
				<xsl:value-of select="string($parentEl/relation_object_order)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:choose>
			<xsl:when test="string($parentEl/relation_effective_from) != ''">
				<xsl:attribute name="effectiveFrom">
					<xsl:value-of select="string($parentEl/relation_effective_from)" />
				</xsl:attribute>
			</xsl:when>
			<xsl:when test="string($base/effective_from) != ''">
				<xsl:attribute name="effectiveFrom">
					<xsl:value-of select="string($base/effective_from)" />
				</xsl:attribute>
			</xsl:when>
		</xsl:choose>		
		<xsl:if test="string($parentEl/effective_to) != ''">
			<xsl:attribute name="effectiveTo">
				<xsl:value-of select="string($parentEl/effective_to)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:choose>
			<xsl:when test="string($parentEl/zone_perm_id) != ''">
				<xsl:value-of select="string($parentEl/zone_perm_id)" />
			</xsl:when>
			<xsl:when test="string($parentEl/gun_perm_id) != ''">
				<xsl:value-of select="string($parentEl/gun_perm_id)" />
			</xsl:when>
		</xsl:choose>		
	</xsl:template>

	<xsl:template name="el_relation_id">
		<xsl:param name="el" select="/.." />
		<xsl:if test="string($el/@relation_object_type_id) != ''">
			<xsl:attribute name="relationObjectTypeId">
				<xsl:value-of select="string($el/@relation_object_type_id)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($el/@relation_object_type) != ''">
			<xsl:attribute name="relationObjectType">
				<xsl:value-of select="string($el/@relation_object_type)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($el/@relationship_id) != ''">
			<xsl:attribute name="relationshipId">
				<xsl:value-of select="string($el/@relationship_id)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($el/@relationship_type_id) != ''">
			<xsl:attribute name="relationshipTypeId">
				<xsl:value-of select="string($el/@relationship_type_id)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($el/@related_object_id) != ''">
			<xsl:attribute name="relatedObjectId">
				<xsl:value-of select="string($el/@related_object_id)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($el/@related_object_type_id) != ''">
			<xsl:attribute name="relatedObjectTypeId">
				<xsl:value-of select="string($el/@related_object_type_id)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($el/@related_object_type) != ''">
			<xsl:attribute name="relatedObjectType">
				<xsl:value-of select="string($el/@related_object_type)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($el/@relation_role) != ''">
			<xsl:attribute name="relationRole">
				<xsl:value-of select="string($el/@relation_role)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($el/@relationship_type) != ''">
			<xsl:attribute name="relationshipType">
				<xsl:value-of select="string($el/@relationship_type)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($el/@relationship_confidence) != ''">
			<xsl:attribute name="relationshipConfidence">
				<xsl:value-of select="string($el/@relationship_confidence)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($el/@relation_object_na_code) != ''">
			<xsl:attribute name="relationObjectNACode">
				<xsl:value-of select="string($el/@relation_object_na_code)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($el/@related_object_order) != ''">
			<xsl:attribute name="relatedObjectOrder">
				<xsl:value-of select="string($el/@related_object_order)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($el/@relation_object_order) != ''">
			<xsl:attribute name="relationObjectOrder">
				<xsl:value-of select="string($el/@relation_object_order)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($el/@effective_from) != ''">
			<xsl:attribute name="effectiveFrom">
				<xsl:value-of select="string($el/@effective_from)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:if test="string($el/@effective_to) != ''">
			<xsl:attribute name="effectiveTo">
				<xsl:value-of select="string($el/@effective_to)" />
			</xsl:attribute>
		</xsl:if>
		<xsl:value-of select="string($el)" />
	</xsl:template>

</xsl:stylesheet>
