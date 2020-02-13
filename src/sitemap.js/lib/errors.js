"use strict";
/*!
 * Sitemap
 * Copyright(c) 2011 Eugene Kalinin
 * MIT Licensed
 */
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * URL in SitemapItem does not exists
 */
class NoURLError extends Error {
    constructor(message) {
        super(message || 'URL is required');
        this.name = 'NoURLError';
        Error.captureStackTrace(this, NoURLError);
    }
}
exports.NoURLError = NoURLError;
/**
 * Config was not passed to SitemapItem constructor
 */
class NoConfigError extends Error {
    constructor(message) {
        super(message || 'SitemapItem requires a configuration');
        this.name = 'NoConfigError';
        Error.captureStackTrace(this, NoConfigError);
    }
}
exports.NoConfigError = NoConfigError;
/**
 * changefreq property in sitemap is invalid
 */
class ChangeFreqInvalidError extends Error {
    constructor(message) {
        super(message || 'changefreq is invalid');
        this.name = 'ChangeFreqInvalidError';
        Error.captureStackTrace(this, ChangeFreqInvalidError);
    }
}
exports.ChangeFreqInvalidError = ChangeFreqInvalidError;
/**
 * priority property in sitemap is invalid
 */
class PriorityInvalidError extends Error {
    constructor(message) {
        super(message || 'priority is invalid');
        this.name = 'PriorityInvalidError';
        Error.captureStackTrace(this, PriorityInvalidError);
    }
}
exports.PriorityInvalidError = PriorityInvalidError;
/**
 * SitemapIndex target Folder does not exists
 */
class UndefinedTargetFolder extends Error {
    constructor(message) {
        super(message || 'Target folder must exist');
        this.name = 'UndefinedTargetFolder';
        Error.captureStackTrace(this, UndefinedTargetFolder);
    }
}
exports.UndefinedTargetFolder = UndefinedTargetFolder;
class InvalidVideoFormat extends Error {
    constructor(message) {
        super(message ||
            'must include thumbnail_loc, title and description fields for videos');
        this.name = 'InvalidVideoFormat';
        Error.captureStackTrace(this, InvalidVideoFormat);
    }
}
exports.InvalidVideoFormat = InvalidVideoFormat;
class InvalidVideoDuration extends Error {
    constructor(message) {
        super(message || 'duration must be an integer of seconds between 0 and 28800');
        this.name = 'InvalidVideoDuration';
        Error.captureStackTrace(this, InvalidVideoDuration);
    }
}
exports.InvalidVideoDuration = InvalidVideoDuration;
class InvalidVideoDescription extends Error {
    constructor(message) {
        super(message || 'description must be no longer than 2048 characters');
        this.name = 'InvalidVideoDescription';
        Error.captureStackTrace(this, InvalidVideoDescription);
    }
}
exports.InvalidVideoDescription = InvalidVideoDescription;
class InvalidVideoRating extends Error {
    constructor(message) {
        super(message || 'rating must be between 0 and 5');
        this.name = 'InvalidVideoRating';
        Error.captureStackTrace(this, InvalidVideoRating);
    }
}
exports.InvalidVideoRating = InvalidVideoRating;
class InvalidAttrValue extends Error {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    constructor(key, val, validator) {
        super('"' +
            val +
            '" tested against: ' +
            validator +
            ' is not a valid value for attr: "' +
            key +
            '"');
        this.name = 'InvalidAttrValue';
        Error.captureStackTrace(this, InvalidAttrValue);
    }
}
exports.InvalidAttrValue = InvalidAttrValue;
// InvalidAttr is only thrown when attrbuilder is called incorrectly internally
/* istanbul ignore next */
class InvalidAttr extends Error {
    constructor(key) {
        super('"' + key + '" is malformed');
        this.name = 'InvalidAttr';
        Error.captureStackTrace(this, InvalidAttr);
    }
}
exports.InvalidAttr = InvalidAttr;
class InvalidNewsFormat extends Error {
    constructor(message) {
        super(message ||
            'must include publication, publication name, publication language, title, and publication_date for news');
        this.name = 'InvalidNewsFormat';
        Error.captureStackTrace(this, InvalidNewsFormat);
    }
}
exports.InvalidNewsFormat = InvalidNewsFormat;
class InvalidNewsAccessValue extends Error {
    constructor(message) {
        super(message ||
            'News access must be either Registration, Subscription or not be present');
        this.name = 'InvalidNewsAccessValue';
        Error.captureStackTrace(this, InvalidNewsAccessValue);
    }
}
exports.InvalidNewsAccessValue = InvalidNewsAccessValue;
class XMLLintUnavailable extends Error {
    constructor(message) {
        super(message || 'xmlLint is not installed. XMLLint is required to validate');
        this.name = 'XMLLintUnavailable';
        Error.captureStackTrace(this, XMLLintUnavailable);
    }
}
exports.XMLLintUnavailable = XMLLintUnavailable;
